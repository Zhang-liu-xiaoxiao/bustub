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
#include "common/logger.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // LOG_DEBUG(" Frame_nums:%zu,k_:%zu", num_frames, k_);
  start_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto cur_time = std::chrono::system_clock::now().time_since_epoch().count();
  if (curr_size_ == 0) {
    return false;
  }
  frame_id_t victim = -1;
  size_t distance = -1;
  auto history_removed = history_.end();
  for (auto it = history_.begin(); it != history_.end(); it++) {
    BUSTUB_ASSERT(static_cast<size_t>(frames_[*it].first) < k_, "error");
    frame_id_t id = *it;
    if (frames_[id].second) {
      if (frame_visit_time_[id][1] < distance) {
        victim = id;
        distance = frame_visit_time_[id][1];
        history_removed = it;
      }
    }
  }
  if (victim != -1) {
    *frame_id = victim;
    history_.erase(history_removed);
    frames_.erase(victim);
    frame_visit_time_.erase(victim);
    curr_size_--;
    // LOG_DEBUG(" Evict:%d", victim);
    return true;
  }

  victim = -1;
  size_t time_gap = 0;
  auto it = cached_.begin();
  auto victim_it = cached_.end();
  for (auto i : cached_) {
    if (frames_[i].second) {
      BUSTUB_ASSERT(static_cast<size_t>(frames_[i].first) >= k_, "error");
      size_t cur_gap = cur_time - frame_visit_time_[i][frames_[i].first - static_cast<int>(k_) + 1];
      if (cur_gap > time_gap) {
        time_gap = cur_gap;
        victim = i;
        victim_it = it;
      }
    }
    it++;
  }
  if (victim != -1) {
    *frame_id = victim;
    cached_.erase(victim_it);
    frames_.erase(victim);
    frame_visit_time_.erase(victim);
    curr_size_--;
    // LOG_DEBUG(" Evict:%d", victim);

    return true;
  }
  // LOG_DEBUG("None available Evict:%d", victim);

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "wrong frame id");
  // LOG_DEBUG("# Access: %d", frame_id);
  std::scoped_lock<std::mutex> lock(latch_);
  auto time_stamp = std::chrono::system_clock::now().time_since_epoch().count();
  if (frames_.find(frame_id) == frames_.end()) {
    frames_[frame_id] = std::make_pair(0, false);
    history_.push_back(frame_id);
  }
  if (static_cast<size_t>(frames_[frame_id].first) < k_) {
    auto find = std::find_if(history_.begin(), history_.end(), [frame_id](frame_id_t id) { return id == frame_id; });
    BUSTUB_ASSERT(find != history_.end(), "frame in wrong list,expected int History");
    if (static_cast<size_t>(frames_[frame_id].first) == k_ - 1) {
      history_.erase(find);
      cached_.push_back(frame_id);
    }
  } else if (static_cast<size_t>(frames_[frame_id].first) >= k_) {
    //! already in cache;
    auto it = std::find_if(cached_.begin(), cached_.end(), [frame_id](frame_id_t id) { return id == frame_id; });
    BUSTUB_ASSERT(it != cached_.end(), "frame in wrong list,expected int cached");
  }
  frames_[frame_id].first++;
  frame_visit_time_[frame_id][frames_[frame_id].first] = time_stamp;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(size_t(frame_id) <= replacer_size_, "wrong frame id");
  // LOG_DEBUG("# SetEvictable: %d,%d", frame_id, set_evictable);
  std::scoped_lock<std::mutex> lock(latch_);
  if (frames_.find(frame_id) == frames_.end()) {
    return;
  }
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
  // LOG_DEBUG("# Remove: %d", frame_id);

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
  frame_visit_time_.erase(frame_id);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
