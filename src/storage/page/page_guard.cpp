#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ != nullptr && bpm_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    Drop();
    // Transfer ownership
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    // Invalidate the moved-from object
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
  }

  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ == nullptr || bpm_ == nullptr) {
    throw std::runtime_error("Cannot upgrade a BasicPageGuard that does not guard any page.");
  }

  ReadPageGuard read_guard(bpm_, page_);

  page_->RLatch();

  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;

  return read_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (page_ == nullptr || bpm_ == nullptr) {
    throw std::runtime_error("Cannot upgrade a BasicPageGuard that does not guard any page.");
  }

  WritePageGuard write_guard(bpm_, page_);

  page_->WLatch();

  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;

  return write_guard;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)){};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
    guard_.page_ = nullptr;
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)){};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr && guard_.bpm_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
    guard_.page_ = nullptr;
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
