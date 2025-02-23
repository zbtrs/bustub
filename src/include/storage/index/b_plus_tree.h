//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // expose for test purpose
  auto FindLeafPage(const KeyType &key, int opt, Transaction *transaction = nullptr, bool leftMost = false)
      -> BPlusTreePage *;

 private:
  void StartNewTree(const KeyType &key, const ValueType &value);

  auto InsertIntoLeaf(BPlusTreePage *node, const KeyType &key, const ValueType &value,
                      Transaction *transaction = nullptr) -> bool;

  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, page_id_t new_page_id,
                        Transaction *transaction = nullptr);

  auto Split(BPlusTreePage *node) -> BPlusTreePage *;

  template <typename N>
  auto CoalesceOrRedistribute(N *node, KeyType min_key, Transaction *transaction = nullptr) -> bool;

  template <typename N>
  auto Coalesce(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int opt,
                int index, Transaction *transaction = nullptr) -> bool;

  template <typename N>
  void Redistribute(N *neighbor_node, N *node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page,
                    int opt, int index);

  auto AdjustRoot(BPlusTreePage *node) -> bool;

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  std::mutex root_latch_;
  void RecursiveUpdate(KeyType min_key, const KeyType &key, page_id_t page_id);
  void RemoveParent(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page, int index);
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *FindCertainLeafPage(int opt);
  void RLatch(BPlusTreePage *child_page);
  void RUnLatch(BPlusTreePage *child_page);
  void WLatch(BPlusTreePage *child_page);
  void WUnLatch(BPlusTreePage *child_page);
};

}  // namespace bustub
