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
  if (index < 0 ) {
    return ;
  }
  array_[index].first = key;
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
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Insert(KeyType key, ValueType value,
                                                                  KeyComparator comparator) -> bool {
  int index = GetSize();
  for (int i = 1; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first) < 0) {
      index = i;
      break;
    } else if (comparator(key, array_[i].first) > 0) {
      continue;
    } else {
      return false;
    }
  }
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::move(std::make_pair(key, value));
  IncreaseSize(1);
  return true;

}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  if (index < 0 ) {
    return ;
  }
  array_[index].second = value;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::ClearAt(int index) {
  if (index < 0) {
    return;
  }
  array_[index] = {};
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
