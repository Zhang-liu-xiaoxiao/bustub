#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

// #pragma ide diagnostic ignored "UnreachableCode"
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  return page->GetSize() == 0;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto page = TraversalsPage(key);
  bool res;
  res = page->SearchKey(key, result, comparator_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {
    auto page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  auto leaf = TraversalsPage(key);
  auto exist = leaf->Insert(key, value, comparator_);
  if (exist) {
    return false;
  }
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);

  if (leaf->GetSize() >= leaf->GetMaxSize()) {
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
    KeyType up_insert_key = leaf->KeyAt(leaf_max_size_ / 2);
    page_id_t parent_id = leaf->GetParentPageId();
    InternalPage *parent_page = nullptr;
    if (parent_id == INVALID_PAGE_ID) {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&parent_id)->GetData());
      parent_page->Init(parent_id, INVALID_PAGE_ID, internal_max_size_);
      parent_page->SetKeyAt(0, {});
      parent_page->SetValueAt(0, leaf->GetPageId());
      leaf->SetParentPageId(parent_id);
      root_page_id_ = parent_id;
    } else {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    }

    new_page->Init(new_page_id, parent_id, leaf_max_size_);
    new_page->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_page_id);
    TransferLeafData(leaf, new_page);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    InsertInternal(up_insert_key, parent_page, new_page_id);
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::TraversalsPage(KeyType key) -> LeafPage * {
  page_id_t next_page_id = root_page_id_;
  while (true) {
    auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
    if (page->IsLeafPage()) {
      return reinterpret_cast<LeafPage *>(page);
    }
    auto internal_page = reinterpret_cast<InternalPage *>(page);
    int index = internal_page->GetSize() - 1;
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(key, internal_page->KeyAt(i)) < 0) {
        index = i - 1;
        break;
      }
    }
    next_page_id = internal_page->ValueAt(index);
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  }
}

//! move data from the full leaf to created empty leaf
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::TransferLeafData(BPlusTree::LeafPage *old_page,
                                                                    BPlusTree::LeafPage *empty_page) {
  for (int i = leaf_max_size_ / 2; i < leaf_max_size_; ++i) {
    empty_page->SetKeyAt(i - (leaf_max_size_ / 2), old_page->KeyAt(i));
    empty_page->SetValueAt(i - (leaf_max_size_ / 2), old_page->ValueAt(i));
  }
  old_page->SetSize(leaf_max_size_ / 2);
  empty_page->SetSize(leaf_max_size_ - leaf_max_size_ / 2);
}

//! internal recursive insert key
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::InsertInternal(KeyType key, InternalPage *page,
                                                                  page_id_t inserted_page) {
  auto res = page->Insert(key, inserted_page, comparator_);
  if (!res) {
    LOG_ERROR("Internal page insert key,value:{%d} error! Duplicated key", inserted_page);
    return;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  if (page->GetSize() > internal_max_size_) {
    page_id_t new_page_id;
    auto new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
    KeyType up_insert_key = page->KeyAt((internal_max_size_ + 1) / 2);
    page_id_t parent_id = page->GetParentPageId();
    InternalPage *parent_page = nullptr;
    if (parent_id == INVALID_PAGE_ID) {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&parent_id)->GetData());
      parent_page->Init(parent_id, INVALID_PAGE_ID, internal_max_size_);
      parent_page->SetKeyAt(0, {});
      parent_page->SetValueAt(0, page->GetPageId());

      page->SetParentPageId(parent_id);
      root_page_id_ = parent_id;
    } else {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    }
    new_internal_page->Init(new_page_id, parent_id, internal_max_size_);
    TransferInternalData(page, new_internal_page);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    InsertInternal(up_insert_key, parent_page, new_page_id);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::TransferInternalData(BPlusTree::InternalPage *old_page,
                                                                        BPlusTree::InternalPage *empty_page) {
  auto old_remain = (internal_max_size_ + 1) / 2;
  auto move_size = internal_max_size_ + 1 - old_remain;
  for (int i = 0; i < move_size; ++i) {
    empty_page->SetKeyAt(i, old_page->KeyAt(i + old_remain));
    empty_page->SetValueAt(i, old_page->ValueAt(i + old_remain));
    auto page = reinterpret_cast<BPlusTreePage *>(
        buffer_pool_manager_->FetchPage(old_page->ValueAt(i + old_remain))->GetData());
    page->SetParentPageId(empty_page->GetPageId());
    old_page->ClearAt(i + old_remain);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  old_page->SetSize(old_remain);
  empty_page->SetSize(move_size);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
