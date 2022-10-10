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
#include <algorithm>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = history_.begin(); it != history_.end(); it++) {
    frame_id_t id = *it;
    if (frames_[id].second) {
      *frame_id = id;
      history_.erase(it);
      frames_.erase(id);
      curr_size_--;
      return true;
    }
  }

  for (auto it = cached_.begin(); it != cached_.end(); it++) {
    frame_id_t id = *it;
    if (frames_[id].second) {
      *frame_id = id;
      cached_.erase(it);
      frames_.erase(id);
      curr_size_--;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "wrong frame id");
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_.find(frame_id) == frames_.end()) {
    frames_[frame_id] = std::make_pair(0, false);
    history_.push_back(frame_id);
  }
  if (static_cast<size_t>(frames_[frame_id].first) < k_) {
    auto find = std::find_if(history_.begin(), history_.end(), [frame_id](frame_id_t id) { return id == frame_id; });
    BUSTUB_ASSERT(find != history_.end(), "frame in wrong list,expected int cached");
    history_.erase(find);
    if (static_cast<size_t>(frames_[frame_id].first) == k_ - 1) {
      cached_.push_back(frame_id);
    } else {
      history_.push_back(frame_id);
    }
  } else if (static_cast<size_t>(frames_[frame_id].first) >= k_) {
    //! already in cache;
    auto it = std::find_if(cached_.begin(), cached_.end(), [frame_id](frame_id_t id) { return id == frame_id; });
    BUSTUB_ASSERT(it != cached_.end(), "frame in wrong list,expected int cached");
    cached_.erase(it);
    cached_.push_back(frame_id);
  }
  frames_[frame_id].first++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "wrong frame id");
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frames_.find(frame_id) != frames_.end(), "Set frame evict not exist");
  if (set_evictable != frames_[frame_id].second) {
    frames_[frame_id].second = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "wrong frame id");
  if (frames_.find(frame_id) == frames_.end()) {
    return;
  }
  BUSTUB_ASSERT(frames_[frame_id].second == true, "frame cannot evicted!");
  if (static_cast<size_t>(frames_[frame_id].first) < k_) {
    auto find = std::find_if(history_.begin(), history_.end(), [frame_id](frame_id_t id) { return id == frame_id; });
    BUSTUB_ASSERT(find != history_.end(), "frame in wrong list,expected int cached");
    history_.erase(find);
  } else {
    auto find = std::find_if(cached_.begin(), cached_.end(), [frame_id](frame_id_t id) { return id == frame_id; });
    BUSTUB_ASSERT(find != cached_.end(), "frame in wrong list,expected int cached");
    cached_.erase(find);
  }
  frames_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
