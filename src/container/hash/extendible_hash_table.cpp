//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <list>
#include <set>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  for (int i = 0; i < num_buckets_; i++) {
    dir_.push_back(std::make_shared<Bucket>(bucket_size));
    bucket_locks_.push_back(std::make_unique<std::mutex>());
  }
  index_2_lock_[0] = 0;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket_index = IndexOf(key);
  //  std::scoped_lock<std::mutex> lock(*bucket_locks_[index_2_lock_[bucket_index]]);
  std::shared_ptr<Bucket> bucket = dir_[bucket_index];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  auto bucket_index = IndexOf(key);
  //  std::scoped_lock<std::mutex> lock(*bucket_locks_[index_2_lock_[bucket_index]]);
  std::shared_ptr<Bucket> bucket = dir_[bucket_index];
  return bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  {
    std::scoped_lock<std::mutex> lock(latch_);
    auto bucket_index = IndexOf(key);
    std::shared_ptr<Bucket> bucket = dir_[bucket_index];
    //    std::scoped_lock<std::mutex> lock(*bucket_locks_[index_2_lock_[bucket_index]]);
    if (bucket->Insert(key, value)) {
      return;
    }
    //    std::scoped_lock<std::mutex> lock_latch(latch_);
    RedistributeBucket(bucket, bucket_index);
  }
  Insert(key, value);
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::RedistributeBucket(std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> bucket,
                                                   size_t bucket_index) -> void {
  auto list = bucket->GetItems();
  std::list<std::pair<K, V>> kv_list;
  kv_list.assign(list.begin(), list.end());
  bucket->GetItems().clear();
  if (bucket->GetDepth() == global_depth_) {
    global_depth_ = global_depth_ + 1;
    size_t old_size = dir_.size();
    dir_.resize(dir_.size() * 2);
    //! 多出一倍的dir位置
    //! 其实就是位置索引的首位多了一个 1
    //! 比如原来global depth是3 现在是4
    //! 多出来1XXX的8个位置,多的位置该放置的bucket和dir原来低位（这里是三位）存放的bucket是一样的
    for (size_t i = old_size; i < dir_.size(); ++i) {
      dir_[i] = dir_[i & ((1 << (global_depth_ == 0 ? 0 : (global_depth_ - 1))) - 1)];
    }
  }
  size_t index_for_old_bucket;
  size_t index_for_new_bucket;
  if (bucket->GetDepth() == 0) {
    index_for_old_bucket = 0;
    index_for_new_bucket = 1;
  } else {
    size_t mask = (1 << bucket->GetDepth()) - 1;
    index_for_old_bucket = bucket_index & mask;
    index_for_new_bucket = index_for_old_bucket | (1 << bucket->GetDepth());
  }
  bucket->IncrementDepth();
  auto new_bucket = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth());
  //! 注意到其实每次扩容只会影响到一个bucket,也就是新建一个,
  //! 比如扩容前 bucket是 depth 3  001,扩容后depth 4, old bucket = 0001 新的是 1001
  for (size_t i = 0; i < dir_.size(); i++) {
    size_t new_mask = (1 << bucket->GetDepth()) - 1;
    if (((i ^ index_for_new_bucket) & new_mask) == 0) {
      dir_[i] = new_bucket;
      //      break;
    }
  }
  for (const auto &p : kv_list) {
    auto i = IndexOf(p.first);
    dir_[i]->Insert(p.first, p.second);
  }
  num_buckets_ += 1;
  bucket_locks_.clear();
  for (int i = 0; i < num_buckets_; ++i) {
    bucket_locks_.push_back(std::make_unique<std::mutex>());
  }
  std::unordered_map<std::shared_ptr<Bucket>, int> buck_2_real_index;
  int count = 0;
  int k = 0;
  for (const auto &i : dir_) {
    if (buck_2_real_index.find(i) == buck_2_real_index.end()) {
      buck_2_real_index[i] = count;
      count++;
      // new bucket appear
    }
    index_2_lock_[k] = buck_2_real_index[i];
    k++;
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto it = std::find_if(list_.begin(), list_.end(), [key](auto pair) { return pair.first == key; });
  if (it == list_.end()) {
    return false;
  }
  value = it->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto it = std::remove_if(list_.begin(), list_.end(), [key](std::pair<K, V> item) { return item.first == key; });
  if (it == list_.end()) {
    return false;
  }
  list_.erase(it, list_.end());
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  //  UNREACHABLE("not implemented");
  auto it = std::remove_if(list_.begin(), list_.end(), [key](std::pair<K, V> item) { return item.first == key; });
  if (it != list_.end()) {
    // exist before, but removed already just push back
    list_.erase(it, list_.end());
    list_.push_back(std::make_pair(key, value));
  } else {
    if (IsFull()) {
      return false;
    }
    list_.push_back(std::make_pair(key, value));
  }
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
