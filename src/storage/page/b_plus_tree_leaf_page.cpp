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
  next_page_id_ = -1;
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
  if (index < 0) {
    LOG_ERROR("Leaf Access Wrong index {%d}, cur array size:{%d}", index, GetSize());
    return {};
  }
  KeyType key{};
  key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ClearAt(int index) {
  if (index < 0) {
    return;
  }
  array_[index] = {};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(KeyType key, KeyComparator comparator) {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(key, array_[i].first) == 0) {
      RemovePairAt(i);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemovePairAt(int index) {
  assert(index >= 0 && index < GetSize());
  ClearAt(index);
  for (int i = index; i < GetSize() - 1; ++i) {
    array_[i] = std::move(array_[i + 1]);
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyExist(KeyType key, KeyComparator comparator) -> bool {
  if (GetSize() == 0) {
    return false;
  }
  return static_cast<bool>(KeyIndex(key, comparator) < GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(KeyType key, KeyComparator comparator) -> int {
  assert(GetSize() > 0);

  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(array_[mid].first, key) == 0) {
      return mid;
    }
    if (comparator(key, array_[mid].first) < 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::Insert(KeyType key, ValueType value,
                                                                  KeyComparator comparator) -> bool {
  if (KeyExist(key, comparator)) {
    return true;
  }
  int index = LookUp(key, comparator);
  for (int i = GetSize(); i > index; --i) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::move(std::make_pair(key, value));
  IncreaseSize(1);

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::ValueAt(int index) const -> ValueType {
  if (index < 0) {
    LOG_ERROR("Leaf Access Wrong index {%d}, cur array size:{%d}", index, GetSize());
    return {};
  }
  ValueType value = array_[index].second;
  return value;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetKeyAt(int index, const KeyType &key) {
  if (index < 0) {
    return;
  }
  array_[index].first = key;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SetValueAt(int index, const ValueType &value) {
  if (index < 0) {
    return;
  }
  array_[index].second = value;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::SearchKey(const KeyType &key, std::vector<ValueType> *result,
                                                                     KeyComparator comparator) -> bool {
  if (GetSize() == 0) {
    return false;
  }
  int index = KeyIndex(key, comparator);
  if (index != GetSize()) {
    result->push_back(array_[index].second);
    return true;
  }
  return false;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::PairAt(int index) -> const std::pair<KeyType, ValueType> & {
  if (index < 0 || index >= GetSize()) {
    assert(false);
  }
  return array_[index];
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::LookUp(KeyType key, KeyComparator comparator) -> int {
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      right = mid - 1;
    } else if (comparator(key, array_[mid].first) >= 0) {
      left = mid + 1;
    }
  }
  return left;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
