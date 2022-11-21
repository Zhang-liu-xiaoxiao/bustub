/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return (page_ == nullptr && (index_ == -1)); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->PairAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ < (page_->GetSize() - 1)) {
    index_++;
    return *this;
  }
  if (page_->GetNextPageId() == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    this->page_ = nullptr;
    this->index_ = -1;

  } else {
    auto next_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(
        buffer_pool_manager_->FetchPage(page_->GetNextPageId())->GetData());
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    this->page_ = next_page;
    this->index_ = 0;
  }
  return *this;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType, ValueType, KeyComparator>::~IndexIterator() {
  if (page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
