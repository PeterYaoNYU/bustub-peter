//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header = header_guard.As<ExtendibleHTableHeaderPage>();

  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  header_guard.Drop();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory = directory_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  directory_guard.Drop();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  V value;
  bool found = bucket->Lookup(key, value, cmp_);
  if (found) {
    result->push_back(value);
  }
  return found;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);

  ExtendibleHTableDirectoryPage *directory;
  WritePageGuard directory_guard;
  if (directory_page_id == INVALID_PAGE_ID) {
    printf("hashed to a new directory page\n");
    // create a new directory page
    BasicPageGuard directory_guard_basic = bpm_->NewPageGuarded(&directory_page_id);
    directory_guard = directory_guard_basic.UpgradeWrite();
    directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory->Init(directory_max_depth_);
    header->SetDirectoryPageId(directory_idx, directory_page_id);
  } else {
    // fetch the directory page that already exists
    directory_guard = bpm_->FetchPageWrite(directory_page_id);
    directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  }
  // drop the header page
  header_guard.Drop();

  // try to find the corresponding bucket page
  ExtendibleHTableBucketPage<K, V, KC> *bucket;
  WritePageGuard bucket_guard;
  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    // create a new bucket page
    BasicPageGuard bucket_guard_basic = bpm_->NewPageGuarded(&bucket_page_id);
    bucket_guard = bucket_guard_basic.UpgradeWrite();
    bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket->Init(bucket_max_size_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    directory->SetLocalDepth(bucket_idx, 0);
  } else {
    bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  // Do not need to drop the directory page now, because we may change it later

  V old_value;
  if (bucket->Lookup(key, old_value, cmp_)) {
    return false;
  }

  if (bucket->IsFull()) {
    if (directory->GetGlobalDepth() == directory->GetLocalDepth(bucket_idx)) {
      if (directory->GetGlobalDepth() >= directory->GetMaxDepth()) {
        return false;
      }
      directory->IncrGlobalDepth();
    }
    directory->IncrLocalDepth(bucket_idx);

    if (!SplitBucket(directory, bucket, bucket_idx)) {
      return false;
    }
    directory_guard.Drop();
    bucket_guard.Drop();
    return Insert(key, value, transaction);
  }
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  return false;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  uint32_t directory_idx = header->HashToDirectoryIndex(Hash(key));
  page_id_t directory_page_id = header->GetDirectoryPageId(directory_idx);
  header_guard.Drop();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory->HashToBucketIndex(Hash(key));
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (!bucket->Remove(key, cmp_)) {
    return false;
  }
  
  if (bucket->IsEmpty()){
    PotentialMergeBucket(directory, bucket, bucket_idx);
  }

  while (directory->CanShrink()){
    directory->DecrGlobalDepth();
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitBucket(ExtendibleHTableDirectoryPage *directory,
                                                    ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx)
    -> bool {
  page_id_t split_page_id;
  BasicPageGuard split_bucket_guard_basic = bpm_->NewPageGuarded(&split_page_id);
  if (split_page_id == INVALID_PAGE_ID) {
    return false;
  }
  // upgrade the guard to write, to hold the write latch
  WritePageGuard split_bucket_guard = split_bucket_guard_basic.UpgradeWrite();
  auto split_bucket = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  split_bucket->Init(bucket_max_size_);

  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  uint32_t split_idx = directory->GetSplitImageIndex(bucket_idx);
  directory->SetBucketPageId(split_idx, split_page_id);
  directory->SetLocalDepth(split_idx, local_depth);

  // update all pointers in the directory page
  uint32_t idx_diff = 1 << local_depth;
  // first update all buckets that correspond to the original bucket
  for (int i = bucket_idx - idx_diff; i >= 0; i -= idx_diff) {
    directory->SetLocalDepth(i, local_depth);
  }
  for (int i = bucket_idx + idx_diff; i < static_cast<int>(directory->Size()); i += idx_diff) {
    directory->SetLocalDepth(i, local_depth);
  }
  for (int i = split_idx - idx_diff; i >= 0; i -= idx_diff) {
    directory->SetLocalDepth(i, local_depth + 1);
    directory->SetBucketPageId(i, split_page_id);
  }
  for (int i = split_idx + idx_diff; i < static_cast<int>(directory->Size()); i += idx_diff) {
    directory->SetLocalDepth(i, local_depth + 1);
    directory->SetBucketPageId(i, split_page_id);
  }

  // migrate entries from the old bucket to the new bucket
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  int size = bucket->Size();
  std::list<std::pair<K, V>> entries;
  for (int i = 0; i < size; i++) {
    entries.push_back(bucket->EntryAt(i));
  }
  bucket->Clear();

  // redistribute
  for (auto &entry : entries) {
    uint32_t target_idx = directory->HashToBucketIndex(Hash(entry.first));
    page_id_t target_page_id = directory->GetBucketPageId(target_idx);
    assert(target_page_id == bucket_page_id || target_page_id == split_page_id);
    if (target_page_id == bucket_page_id) {
      bucket->Insert(entry.first, entry.second, cmp_);
    } else {
      split_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
