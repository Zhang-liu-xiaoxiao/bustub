//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  // LOG_DEBUG("BPM ,pool size %zu,replacer size:%zu", pool_size, replacer_k);
  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // LOG_DEBUG("New PG");
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  page_id_t allocated_id;
  if (!free_list_.empty()) {
    allocated_id = AllocatePage();
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    allocated_id = AllocatePage();
    page_table_->Remove(pages_[frame_id].page_id_);
    replacer_->Remove(frame_id);
  } else {
    // LOG_DEBUG("New PG Return null");
    return nullptr;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }
  pages_[frame_id].is_dirty_ = false;
  page_table_->Insert(allocated_id, frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_ = allocated_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  *page_id = allocated_id;
  // LOG_DEBUG("New PG return id %d:", allocated_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("Fetch Pg :%d", page_id);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    if (pages_[frame_id].GetPinCount() == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    page_table_->Remove(pages_[frame_id].page_id_);
    replacer_->Remove(frame_id);
  } else {
    // LOG_DEBUG("Fetch page %d return null", page_id);
    return nullptr;
  }
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }
  pages_[frame_id].is_dirty_ = false;
  page_table_->Insert(page_id, frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_++;
  pages_[frame_id].page_id_ = page_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  // LOG_DEBUG("Fetch page %d Success", page_id);
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  // LOG_DEBUG("Unpin %d ,is dirty %d", page_id, is_dirty);
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = 0;
  if ((!page_table_->Find(page_id, frame_id)) || (pages_[frame_id].GetPinCount() == 0)) {
    // LOG_DEBUG("Unpin %d ,is dirty %d Return false", page_id, is_dirty);
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  // LOG_DEBUG("Unpin %d ,is dirty %d return true", page_id, is_dirty);
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("Flush %d", page_id);
  if (page_id == INVALID_PAGE_ID) {
    // LOG_DEBUG("Flush %d fail", page_id);
    return false;
  }
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // LOG_DEBUG("Flush %d fail", page_id);
    return false;
  }
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  pages_[frame_id].is_dirty_ = false;
  // LOG_DEBUG("Flush %d Success", page_id);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // LOG_DEBUG("Flush All Page");
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // LOG_DEBUG("Delete Page %d", page_id);
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = 0;
  if (!page_table_->Find(page_id, frame_id)) {
    // LOG_DEBUG("Delete Page %d, success", page_id);
    return true;
  }
  auto &page = pages_[frame_id];
  if (page.GetPinCount() > 0) {
    // LOG_DEBUG("Delete Page %d, pinned", page_id);
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  DeallocatePage(page_id);
  // LOG_DEBUG("Delete Page %d success", page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
