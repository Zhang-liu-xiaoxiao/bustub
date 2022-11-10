//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetPageType(IndexPageType::INTERNAL_PAGE);
  BPlusTreePage::SetMaxSize(max_size);
  BPlusTreePage::SetSize(1);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 || index >= GetSize()) {
    LOG_ERROR("internal Access Wrong index {%d}, cur array size:{%d}", index, GetSize());
    return {};
  }
  KeyType key{};
  key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0) {
    return;
  }
  array_[index].first = key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemovePairAt(int index) {
  assert(index > 0 && index < GetSize());
  ClearAt(index);
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = std::move(array_[i + 1]);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertHead(KeyType key, ValueType value) {
  auto cur_size = GetSize();
  for (int i = cur_size; i > 0; --i) {
    array_[i] = std::move(array_[i - 1]);
  }
  int j = 1;
  array_[j].first = key;
  array_[0].first = {};
  array_[0].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveHead() {
  auto cur_size = GetSize();
  for (int i = 0; i < cur_size - 1; ++i) {
    array_[i] = std::move(array_[i + 1]);
  }
  array_[0].first = {};
  IncreaseSize(-1);
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return {};
  }
  ValueType value = array_[index].second;
  return value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(KeyType key, ValueType value, KeyComparator comparator) -> bool {
  int index = GetSize();
  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first) < 0) {
      index = i;
      break;
    }
    if (comparator(key, array_[i].first) > 0) {
      continue;
    }
    return false;
  }
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::move(std::make_pair(key, value));
  IncreaseSize(1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  if (index < 0) {
    return;
  }
  array_[index].second = value;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ClearAt(int index) {
  if (index < 0) {
    return;
  }
  array_[index] = {};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKey(KeyType key, KeyComparator comparator) {
  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first) == 0) {
      ClearAt(i);
      for (int j = i; j < GetSize() - 1; ++j) {
        array_[j] = std::move(array_[j + 1]);
      }
      IncreaseSize(-1);
      return;
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(KeyType key, KeyComparator comparator) -> int {
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      right = mid - 1;
    } else if (comparator(key, array_[mid].first) >= 0) {
      left = mid + 1;
    }
  }
  return left - 1;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
