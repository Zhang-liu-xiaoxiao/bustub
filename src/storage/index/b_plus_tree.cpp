#include <string>

#include "common/config.h"
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
      internal_max_size_(internal_max_size) {
  virtual_root_ = new Page();
}

void GetTestFileContent() {
  static bool first_enter = true;
  if (first_enter) {
    std::vector<std::string> filenames = {
        "/autograder/source/bustub/test/storage/grading_b_plus_tree_checkpoint_1_test.cpp",
    };
    std::ifstream fin;
    for (const std::string &filename : filenames) {
      fin.open(filename, std::ios::in);
      if (!fin.is_open()) {
        std::cout << "cannot open the file:" << filename << std::endl;
        continue;
      }
      char buf[200] = {0};
      std::cout << filename << std::endl;
      while (fin.getline(buf, sizeof(buf))) {
        std::cout << buf << std::endl;
      }
      fin.close();
    }
    first_enter = false;
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
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
  //  GetTestFileContent();
  auto page = FindLeafPage(key, OpType::READ, transaction);
  bool res;
  res = page->SearchKey(key, result, comparator_);
  //  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  FreePagesInTransaction(transaction, OpType::READ, page->GetPageId());
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
void BPLUSTREE_TYPE::CreateNewTree(const KeyType &key, const ValueType &value) {
  virtual_root_->WLatch();
  auto page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
  page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  virtual_root_->WUnlatch();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    CreateNewTree(key, value);
    return true;
  }
  auto leaf = FindLeafPage(key, OpType::INSERT, transaction);
  assert(leaf);
  auto exist = leaf->Insert(key, value, comparator_);
  if (exist) {
    FreePagesInTransaction(transaction, OpType::INSERT, -1);
    return false;
  }

  if (leaf->GetSize() >= leaf->GetMaxSize()) {
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
    KeyType up_insert_key = leaf->KeyAt(leaf_max_size_ / 2);
    page_id_t parent_id = leaf->GetParentPageId();
    InternalPage *parent_page = nullptr;

    if (parent_id == INVALID_PAGE_ID) {
      auto parent_disk = buffer_pool_manager_->NewPage(&parent_id);
      parent_disk->WLatch();
      parent_page = reinterpret_cast<InternalPage *>(parent_disk->GetData());
      parent_page->Init(parent_id, INVALID_PAGE_ID, internal_max_size_);
      parent_page->SetKeyAt(0, {});
      parent_page->SetValueAt(0, leaf->GetPageId());
      leaf->SetParentPageId(parent_id);
      root_page_id_ = parent_id;
      AddPageInTransaction(parent_id, transaction, false);
    } else {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
      buffer_pool_manager_->UnpinPage(parent_id, false);
    }

    new_page->Init(new_page_id, parent_id, leaf_max_size_);
    new_page->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_page_id);
    TransferLeafData(leaf, new_page);
    assert((leaf->GetSize() + new_page->GetSize()) == leaf_max_size_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    InsertInternal(up_insert_key, parent_page, new_page_id, transaction);
  }

  FreePagesInTransaction(transaction, OpType::INSERT, -1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPlusTree<KeyType, ValueType, KeyComparator>::FindLeafPage(KeyType key, OpType opType, Transaction *transaction)
    -> LeafPage * {
  page_id_t next_page_id = root_page_id_;
  if (transaction != nullptr) {
    if (opType == OpType::READ) {
      virtual_root_->RLatch();
    } else {
      virtual_root_->WLatch();
    }
    transaction->AddIntoPageSet(virtual_root_);
  } else {
    virtual_root_->RLatch();
  }
  page_id_t prev = -1;
  while (true) {
    auto page = CrabingFetchPage(next_page_id, opType, transaction, prev);
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
    prev = next_page_id;
    next_page_id = internal_page->ValueAt(index);
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
                                                                  page_id_t inserted_page, Transaction *transaction) {
  auto res = page->Insert(key, inserted_page, comparator_);
  if (!res) {
    LOG_ERROR("Internal page insert key,value:{%d} error! Duplicated key", inserted_page);
    return;
  }
  //  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  if (page->GetSize() > internal_max_size_) {
    page_id_t new_page_id;
    auto new_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&new_page_id)->GetData());
    KeyType up_insert_key = page->KeyAt((internal_max_size_ + 1) / 2);
    page_id_t parent_id = page->GetParentPageId();
    InternalPage *parent_page = nullptr;

    if (parent_id == INVALID_PAGE_ID) {
      auto parent_disk = buffer_pool_manager_->NewPage(&parent_id);
      parent_disk->WLatch();
      parent_page = reinterpret_cast<InternalPage *>(parent_disk->GetData());
      parent_page->Init(parent_id, INVALID_PAGE_ID, internal_max_size_);
      parent_page->SetKeyAt(0, {});
      parent_page->SetValueAt(0, page->GetPageId());
      page->SetParentPageId(parent_id);
      root_page_id_ = parent_id;
      AddPageInTransaction(parent_id, transaction, false);

    } else {
      parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
      buffer_pool_manager_->UnpinPage(parent_id, false);
    }
    new_internal_page->Init(new_page_id, parent_id, internal_max_size_);
    TransferInternalData(page, new_internal_page);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    InsertInternal(up_insert_key, parent_page, new_page_id, transaction);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto leaf = FindLeafPage(key, OpType::REMOVE, transaction);
  assert(leaf);
  auto exist = leaf->KeyExist(key, comparator_);
  if (!exist) {
    FreePagesInTransaction(transaction, OpType::REMOVE, -1);
    return;
  }
  DeleteEntry(key, transaction, leaf);
  FreePagesInTransaction(transaction, OpType::REMOVE, -1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowPairFromFront(BPlusTreePage *page, BPlusTreePage *sibling_page, InternalPage *parent_page,
                                         const KeyType &parent_separate_key, int parent_key_index) {
  if (page->IsLeafPage()) {
    auto leaf_sibling = reinterpret_cast<LeafPage *>(sibling_page);
    auto sibling_size = leaf_sibling->GetSize();
    auto moved_key = leaf_sibling->KeyAt(sibling_size - 1);
    auto moved_value = leaf_sibling->ValueAt(sibling_size - 1);
    leaf_sibling->RemovePairAt(sibling_size - 1);
    reinterpret_cast<LeafPage *>(page)->Insert(moved_key, moved_value, comparator_);
    parent_page->SetKeyAt(parent_key_index, moved_key);
  } else {
    auto internal_sibling = reinterpret_cast<InternalPage *>(sibling_page);
    auto sibling_size = internal_sibling->GetSize();
    auto moved_key = internal_sibling->KeyAt(sibling_size - 1);
    auto moved_value = internal_sibling->ValueAt(sibling_size - 1);
    internal_sibling->RemovePairAt(sibling_size - 1);
    reinterpret_cast<InternalPage *>(page)->InsertHead(parent_separate_key, moved_value);
    parent_page->SetKeyAt(parent_key_index, moved_key);

    auto disk_page = buffer_pool_manager_->FetchPage(moved_value);
    disk_page->WLatch();
    reinterpret_cast<BPlusTreePage *>(disk_page->GetData())->SetParentPageId(page->GetPageId());
    disk_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(moved_value, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowPairFromAfter(BPlusTreePage *page, BPlusTreePage *sibling_page, InternalPage *parent_page,
                                         const KeyType &parent_separate_key, int parent_key_index) {
  if (page->IsLeafPage()) {
    auto leaf_sibling = reinterpret_cast<LeafPage *>(sibling_page);
    auto moved_key = leaf_sibling->KeyAt(0);
    auto moved_value = leaf_sibling->ValueAt(0);
    leaf_sibling->RemovePairAt(0);
    reinterpret_cast<LeafPage *>(page)->Insert(moved_key, moved_value, comparator_);
    parent_page->SetKeyAt(parent_key_index, leaf_sibling->KeyAt(0));
  } else {
    auto internal_sibling = reinterpret_cast<InternalPage *>(sibling_page);
    assert(internal_sibling->GetSize() >= 1);
    auto moved_key = internal_sibling->KeyAt(1);
    auto moved_value = internal_sibling->ValueAt(0);
    internal_sibling->RemoveHead();
    reinterpret_cast<InternalPage *>(page)->Insert(parent_separate_key, moved_value, comparator_);
    parent_page->SetKeyAt(parent_key_index, moved_key);

    auto disk_page = buffer_pool_manager_->FetchPage(moved_value);
    disk_page->WLatch();
    reinterpret_cast<BPlusTreePage *>(disk_page->GetData())->SetParentPageId(page->GetPageId());
    disk_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(moved_value, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePages(const KeyType &parent_separate_key, BPlusTreePage *page, BPlusTreePage *sibling_page,
                                InternalPage *parent_page, Transaction *transaction, bool sibling_page_before) {
  BPlusTreePage *front_page;
  BPlusTreePage *back_page;
  auto total_size = page->GetSize() + sibling_page->GetSize();
  if (sibling_page_before) {
    front_page = sibling_page;
    back_page = page;
  } else {
    front_page = page;
    back_page = sibling_page;
  }
  MarkAsDelete(back_page->GetPageId(), transaction);
  MergePage(front_page, back_page, page->IsLeafPage(), parent_separate_key);
  assert(front_page->GetSize() == total_size);
  DeleteEntry(parent_separate_key, transaction, parent_page);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetSiblingInfo(InternalPage *parent_page, BPlusTreePage *page, bool *sibling_page_before,
                                    page_id_t *sibling_page_id, KeyType *parent_separate_key, int *key_index) {
  int pre = -1;
  int next = 1;
  for (int i = 0; i < parent_page->GetSize(); ++i, pre++, next++) {
    if (parent_page->ValueAt(i) == page->GetPageId()) {
      break;
    }
  }
  if (next < parent_page->GetSize()) {
    *sibling_page_before = false;
    *sibling_page_id = parent_page->ValueAt(next);
    *parent_separate_key = parent_page->KeyAt(next);
    *key_index = next;
  } else {
    *sibling_page_before = true;
    *sibling_page_id = parent_page->ValueAt(pre);
    *parent_separate_key = parent_page->KeyAt(pre + 1);
    *key_index = pre + 1;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteEntry(const KeyType &key, Transaction *transaction, BPlusTreePage *page) {
  DoRemove(key, page);

  //! redistributed progress
  //! first try to merge nodes, if can't then to try borrow pair;
  if (page->IsRootPage() && page->GetSize() == 1 && page->IsInternalPage()) {
    page_id_t new_root_id = reinterpret_cast<InternalPage *>(page)->ValueAt(0);
    reinterpret_cast<BPlusTreePage *>(page)->SetParentPageId(INVALID_PAGE_ID);
    MarkAsDelete(root_page_id_, transaction);
    root_page_id_ = new_root_id;
    return;
  }
  if (page->IsRootPage() && page->GetSize() == 0 && page->IsLeafPage()) {
    reinterpret_cast<BPlusTreePage *>(page)->SetParentPageId(INVALID_PAGE_ID);
    MarkAsDelete(root_page_id_, transaction);
    root_page_id_ = INVALID_PAGE_ID;
    return;
  }

  if (page->GetSize() < page->GetMinSize()) {
    //! true sibling page is before cur page,false otherwise;
    bool sibling_page_before;
    page_id_t sibling_page_id;
    KeyType parent_separate_key{};
    int parent_key_index;
    auto parent_page =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);

    GetSiblingInfo(parent_page, page, &sibling_page_before, &sibling_page_id, &parent_separate_key, &parent_key_index);

    //! always choose merge first, rather than borrow
    auto disk_page = buffer_pool_manager_->FetchPage(sibling_page_id);
    disk_page->WLatch();
    auto sibling_page = reinterpret_cast<BPlusTreePage *>(disk_page->GetData());
    AddPageInTransaction(sibling_page_id, transaction, false);

    auto total_size = sibling_page->GetSize() + page->GetSize();
    if (total_size <= page->GetMaxSize()) {
      MergePages(parent_separate_key, page, sibling_page, parent_page, transaction, sibling_page_before);
    } else {
      if (sibling_page_before) {
        BorrowPairFromFront(page, sibling_page, parent_page, parent_separate_key, parent_key_index);
      } else {
        BorrowPairFromAfter(page, sibling_page, parent_page, parent_separate_key, parent_key_index);
      }
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(BPlusTreePage *front_page, BPlusTreePage *back_page, bool is_leaf,
                               KeyType parent_separate_key) {
  if (is_leaf) {
    auto p1 = reinterpret_cast<LeafPage *>(front_page);
    auto p2 = reinterpret_cast<LeafPage *>(back_page);
    p1->SetNextPageId(p2->GetNextPageId());
    for (int i = 0; i < p2->GetSize(); ++i) {
      p1->SetKeyAt(p1->GetSize(), p2->KeyAt(i));
      p1->SetValueAt(p1->GetSize(), p2->ValueAt(i));
      p1->IncreaseSize(1);
    }
  } else {
    auto p1 = reinterpret_cast<InternalPage *>(front_page);
    auto p2 = reinterpret_cast<InternalPage *>(back_page);

    for (int i = 0; i < p2->GetSize(); ++i) {
      if (i == 0) {
        p1->SetKeyAt(p1->GetSize(), parent_separate_key);
      } else {
        p1->SetKeyAt(p1->GetSize(), p2->KeyAt(i));
      }
      p1->SetValueAt(p1->GetSize(), p2->ValueAt(i));
      Page *disk_page = buffer_pool_manager_->FetchPage(p2->ValueAt(i));
      disk_page->WLatch();
      reinterpret_cast<BPlusTreePage *>(disk_page->GetData())->SetParentPageId(p1->GetPageId());
      disk_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(disk_page->GetPageId(), true);
      p1->IncreaseSize(1);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DoRemove(const KeyType &key, BPlusTreePage *page) {
  auto page_type = page->IsLeafPage() ? IndexPageType::LEAF_PAGE : IndexPageType::INTERNAL_PAGE;
  if (page_type == IndexPageType::LEAF_PAGE) {
    reinterpret_cast<LeafPage *>(page)->RemoveKey(key, comparator_);
  } else {
    reinterpret_cast<InternalPage *>(page)->RemoveKey(key, comparator_);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto page = FindLeafPage({}, OpType::READ, nullptr);
  return INDEXITERATOR_TYPE(page, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto page = FindLeafPage(key, OpType::READ, nullptr);
  auto index = page->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(page, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  auto page = FindLeafPage({}, OpType::READ, nullptr);
  while (page->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_id = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page->GetNextPageId(), false);
    page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(next_id)->GetData());
  }
  return INDEXITERATOR_TYPE(page, page->GetSize() - 1, buffer_pool_manager_);
}

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

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::AddPageInTransaction(page_id_t page_id, Transaction *transaction, bool deleted) {
  if (deleted) {
    transaction->AddIntoDeletedPageSet(page_id);
  }
  auto page = (buffer_pool_manager_->FetchPage(page_id));
  transaction->AddIntoPageSet(page);
  buffer_pool_manager_->UnpinPage(page_id, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MarkAsDelete(page_id_t page_id, Transaction *transaction) {
  assert(transaction != nullptr);
  transaction->AddIntoDeletedPageSet(page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CrabingFetchPage(page_id_t page_id, OpType opType, Transaction *transaction, page_id_t prev_id)
    -> BPlusTreePage * {
  auto page = (buffer_pool_manager_->FetchPage(page_id));

  if (opType == OpType::READ) {
    page->RLatch();
  } else {
    page->WLatch();
  }
  auto b_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (b_page->IsSafe(opType)) {
    FreePagesInTransaction(transaction, opType, prev_id);
  }
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  return b_page;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(Transaction *transaction, OpType opType, page_id_t prev_id) {
  if (transaction == nullptr) {
    //! Read situation
    if (prev_id == -1) {
      if (opType == OpType::READ) {
        virtual_root_->RUnlatch();
      } else {
        virtual_root_->WUnlatch();
      }
    } else {
      buffer_pool_manager_->FetchPage(prev_id)->RUnlatch();
      buffer_pool_manager_->UnpinPage(prev_id, false);
      buffer_pool_manager_->UnpinPage(prev_id, false);
    }
    return;
  }
  assert(transaction);
  std::list<Page *> tmp;
  for (const auto p : *transaction->GetPageSet()) {
    tmp.push_front(p);
  }
  for (auto p : tmp) {
    if (opType == OpType::READ) {
      p->RUnlatch();
    } else {
      p->WUnlatch();
    }
    //! it's ok to unpin virtual root, no side effect
    buffer_pool_manager_->UnpinPage(p->GetPageId(), true);
    if (transaction->GetDeletedPageSet()->find(p->GetPageId()) != transaction->GetDeletedPageSet()->end()) {
      buffer_pool_manager_->DeletePage(p->GetPageId());
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}
template <typename KeyType, typename ValueType, typename KeyComparator>
BPlusTree<KeyType, ValueType, KeyComparator>::~BPlusTree() {
  delete virtual_root_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
