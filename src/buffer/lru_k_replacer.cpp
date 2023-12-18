//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (node_store_.empty()) {
    // there is no evictable frame
    return false;
  }

  size_t max_distance = 0;
  frame_id_t candidate = -1;
  bool found_infinite = false;

  for (auto &pair : node_store_) {
    auto &node = pair.second;
    if (!node.is_evictable_ || node.history_.size() < k_) {
      // if it has less than k history elements, its k distance is set to +inf
      if (node.is_evictable_ && node.history_.size() < k_) {
        // there hasn't been an element whose k distance is +inf
        if (!found_infinite) {
          max_distance = std::numeric_limits<size_t>::max();
          candidate = node.fid_;
          found_infinite = true;
        } else {
          // based on LRU, if there are multiple frames with +inf k distance
          // evict the one with the smallest timestamp
          if (node.history_.front() < node_store_[candidate].history_.front()) {
            candidate = node.fid_;
          }
        }
      }
      // continue the execution, skip this node
      continue;
    }

    size_t distance = current_timestamp_ - node.history_.front();

    if (distance > max_distance) {
      max_distance = distance;
      candidate = node.fid_;
    }
  }

  if (candidate != -1) {
    *frame_id = candidate;
    node_store_.erase(candidate);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);

  // If frame id is invalid (ie. larger than replacer_size_), throw an exception.
  if (frame_id >= static_cast<int32_t>(replacer_size_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "frame id is out of range");
  }

  ++current_timestamp_;

  // Check if the frame_id exists in node_store_.
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    // If not found, initialize a new LRUKNode and insert it into node_store_.
    LRUKNode new_node;
    new_node.fid_ = frame_id;
    new_node.history_.push_back(current_timestamp_);
    new_node.k_ = k_;  // Assuming k_ is a property you want to set during initialization.
    node_store_.emplace(frame_id, std::move(new_node));
  } else {
    // If found, update the existing node.
    auto &node = it->second;
    if (node.history_.size() >= k_) {
      node.history_.pop_front();  // Ensure only the last k timestamps are kept.
    }
    node.history_.push_back(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);

  // If frame id is invalid (ie. larger than replacer_size_), throw an exception.
  if (frame_id >= static_cast<int32_t>(replacer_size_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "frame id is out of range");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "Frame ID does not exist.");
  }

  auto &node = it->second;
  if (node.is_evictable_ != set_evictable) {
    node.is_evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // If frame id is invalid (ie. larger than replacer_size_), throw an exception.
  if (frame_id >= static_cast<int32_t>(replacer_size_)) {
    throw Exception(ExceptionType::OUT_OF_RANGE, "frame id is out of range");
  }

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  auto &node = it->second;
  if (node.is_evictable_) {
    curr_size_--;
  }
  node_store_.erase(it);
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
