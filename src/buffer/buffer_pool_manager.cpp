//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    // get the page directly from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    Page &page = pages_[frame_id];
    if (page.IsDirty()) {
      FlushPage(page.GetPageId());
    }
    page_table_.erase(page.GetPageId());
  } else {
    return nullptr;
  }

  *page_id = AllocatePage();

  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.page_id_ = *page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;

  page_table_[*page_id] = frame_id;

  replacer_->Remove(frame_id);
  replacer_->RecordAccess(frame_id, AccessType::Unknown);
  replacer_->SetEvictable(frame_id, false);
  return &page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  auto page_table_it = page_table_.find(page_id);

  if (page_table_it != page_table_.end()) {
    // page is in the buffer pool
    // printf("Fetching page first phase, page found in buffer pool\n");
    frame_id_t frame_id = page_table_it->second;
    Page &page = pages_[frame_id];
    page.pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    replacer_->SetEvictable(frame_id, false);
    return &page;
  }

  printf("Fetching page first phase, page not found in buffer pool\n");

  // page is not in the buffer pool
  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    // get the page directly from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
    printf("get the page from the free list\n");
  } else if (replacer_->Evict(&frame_id)) {
    printf("evict the page from the replacer, frame id is %d\n", frame_id);
    Page &page = pages_[frame_id];
    if (page.IsDirty()) {
      printf("the page evicted is dirty\n");
      FlushPage(page.GetPageId());
    }
    page_table_.erase(page.GetPageId());

    // printf("the page is getting del\n");
    // DeletePage(page.GetPageId());
    // printf("deletion done\n");
  } else {
    return nullptr;
  }

  // reset the metadata of the new page
  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.page_id_ = page_id;
  page.pin_count_ = 1;  // Pin the new page.
  page.is_dirty_ = false;

  // schedule a read request
  DiskRequest read_request;
  read_request.is_write_ = false;
  read_request.data_ = page.data_;
  read_request.page_id_ = page_id;

  // create the Promise and get its future
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  // set the Promise to the callback
  read_request.callback_ = std::move(promise);

  // schedule the read request
  disk_scheduler_->Schedule(std::move(read_request));

  // wait for the read request to finish
  future.wait();

  page_table_[page_id] = frame_id;

  printf("Fetching page last phase\n");

  replacer_->Remove(frame_id);
  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);
  return &page;

  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id = it->second;

  Page &page = pages_[frame_id];

  if (page.pin_count_ <= 0) {
    return false;
  }

  page.pin_count_--;

  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  if (is_dirty) {
    page.is_dirty_ = true;
    // FlushPage(page_id);
  }

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];

  DiskRequest write_request;
  write_request.is_write_ = true;
  write_request.data_ = page.GetData();
  write_request.page_id_ = page.GetPageId();

  // create the Promise and get its future
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  // set the Promise to the callback
  write_request.callback_ = std::move(promise);

  // schedule the write request
  disk_scheduler_->Schedule(std::move(write_request));

  // wait for the write request to finish
  future.wait();
  page.is_dirty_ = false;

  // page_table_.erase(it);
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);
  for (auto &entry : page_table_) {
    FlushPage(entry.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = it->second;
  // printf("deletePage, the page id is %d, and the frame id is %d\n", page_id, frame_id);
  Page &page = pages_[frame_id];

  if (page.pin_count_ > 0) {
    return false;
  }

  if (page.IsDirty()) {
    FlushPage(page_id);
  }

  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page_table_.erase(it);
  // replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
