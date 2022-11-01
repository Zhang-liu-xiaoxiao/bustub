//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetPageType(IndexPageType::LEAF_PAGE);
  BPlusTreePage::SetMaxSize(max_size);
  BPlusTreePage::SetSize(0);

}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 ) {
    LOG_ERROR("Leaf Access Wrong index {%d}, cur array size:{%d}", index, GetSize());
    return {};
  }
  KeyType key{};
  key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Insert(KeyType key, ValueType value,
                                                                  KeyComparator comparator) -> bool {
  int index = GetSize();
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first) < 0) {
      index = i;
      break;
    } else if (comparator(key, array_[i].first) > 0) {
      continue;
    } else {
      return true;
    }
  }
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::move(std::make_pair(key, value));
  IncreaseSize(1);

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const -> ValueType {
  if (index < 0 ) {
    LOG_ERROR("Leaf Access Wrong index {%d}, cur array size:{%d}", index, GetSize());
    return {};
  }
  ValueType value = array_[index].second;
  return value;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 ) {
    return;
  }
  array_[index].first = key;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  if (index < 0 ) {
    return;
  }
  array_[index].second = value;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SearchKey(const KeyType &key, std::vector<ValueType> *result,
                                                                     KeyComparator comparator) -> bool {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, KeyAt(i)) == 0) {
      result->push_back(ValueAt(i));
      return true;
    }
  }
  return false;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
