//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <utility>
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  void SetValueAt(int index, const ValueType &value);
  auto ValueIndex(const ValueType &value) const -> int;
  auto ValueAt(int index) const -> ValueType;

  auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType;
  void PopulateNewRoot(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value);
  auto InsertNodeAfter(const ValueType &old_value, const KeyType &new_key, const ValueType &new_value) -> int;
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int;
  void Remove(int index);
  auto RemoveAndReturnOnlyChild() -> ValueType;

  // Split and Merge utility methods
  void MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key, int opt);
  auto MoveHalfTo(BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) -> KeyType;
  void MoveFirstToEndOf(BPlusTreeInternalPage *recipient, BufferPoolManager *bufferPoolManager);
  void MoveLastToFrontOf(BPlusTreeInternalPage *recipient, BufferPoolManager *bufferPoolManager);

  void UpdateParentPageId(BufferPoolManager *buffer_pool_manager);
  void UpdateNewParentId(page_id_t new_page_id, BufferPoolManager *buffer_pool_manager);

  void FindSiblings(KeyType key, KeyComparator comparator, page_id_t *left_sibling_page_id,
                    page_id_t *right_sibling_page_id, int *index);

  auto LookupKey(const KeyType &key, const KeyComparator &comparator) -> int;

 private:
  void CopyNFrom(MappingType *items, int size);
  void CopyLastFrom(const MappingType &pair);
  void CopyFirstFrom(const MappingType &pair);
  // Flexible array member for page data.
  MappingType array_[0];
  void SetItem(int index, std::pair<KeyType, ValueType> item);
};
}  // namespace bustub
