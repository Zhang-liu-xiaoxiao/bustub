//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager_instance.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

TEST(BPlusTreeTests, DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeTests, SimpleTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(5, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 3, 3);
  GenericKey<8> index_key;
  RID rid;
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {13, 22,  3,   14, 1, 45, 25, 56, 27,  18,  24,
                               25, 123, 231, 11, 6, 78, 13, 23, 141, 425, 241};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::vector<int64_t> remove_keys = {13, 22,  3,   14, 1, 45, 25, 56, 27,  18,  24,
                                      25, 123, 231, 11, 6, 78, 13, 23, 141, 425, 241};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}
TEST(BPlusTreeTests, SimpleTest2) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManagerInstance(5, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  std::vector<int64_t> remove_keys = {1, 2};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeDeleteTests, Insert_Delete_Check) {
  int loop = 100;
  for (int test = 0; test < loop; ++test) {
    // create KeyComparator and index schema
    auto key_schema = ParseCreateStatement("a bigint");
    GenericComparator<64> comparator(key_schema.get());

    DiskManager *disk_manager = new DiskManager("test.db");
    BufferPoolManager *bpm = new BufferPoolManagerInstance(30, disk_manager);
    // create b+ tree
    BPlusTree<GenericKey<64>, RID, GenericComparator<64>> tree("foo_pk", bpm, comparator, 3, 3);
    GenericKey<64> index_key;
    RID rid;
    // create transaction
    Transaction *transaction = new Transaction(0);

    // create and fetch header_page
    page_id_t page_id;
    auto header_page = bpm->NewPage(&page_id);
    (void)header_page;

    //    std::vector<int64_t> keys{5,1,7,6,2,4,3,8};
    std::vector<int64_t> keys;
    int scale = 50;  // at first, set a small number(6-10) to find bug
    for (int i = 0; i < scale; ++i) {
      keys.push_back(i + 1);
    }
    std::shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
    std::cout << "\n--- to insert: ";
    for (auto key : keys) {
      std::cout << key << ' ';
    }
    std::cout << '\n';
    std::vector<int64_t> c_keys = keys;
    std::reverse(c_keys.begin(), c_keys.end());
    for (auto key : keys) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set((int32_t)(key >> 32), value);
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid, transaction);
    }

    std::cout << "--- insert end, delete start ---\n";
    for (auto key : keys) {
      index_key.SetFromInteger(key);
      std::cout << "remove: " << key << '\n';
      tree.Remove(index_key, transaction);
      c_keys.pop_back();
      std::vector<RID> rids;
      // check key & value pairs
      std::cout << "checking remaining key & value ";
      for (auto key2 : c_keys) {
        rids.clear();
        index_key.SetFromInteger(key2);
        tree.GetValue(index_key, &rids);
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key2 & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].GetSlotNum(), value);
      }
      std::cout << " -> ok\n";
    }
    bpm->UnpinPage(HEADER_PAGE_ID, true);
    delete transaction;
    delete disk_manager;
    delete bpm;
    remove("test.db");
    remove("test.log");
  }
}
}  // namespace bustub
