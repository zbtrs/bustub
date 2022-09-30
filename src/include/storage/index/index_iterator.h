//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(page_id_t page_id,page_id_t next_page_id, int size, int cursor, BufferPoolManager *buffer_pool);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return page_id_ == itr.page_id_ && size_ == itr.size_ && cursor_ == itr.cursor_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return static_cast<bool>(page_id_ != itr.page_id_ || size_ != itr.size_ || cursor_ != itr.cursor_);
  }

 private:
  page_id_t page_id_;
  page_id_t next_page_id_;
  int size_;
  int cursor_;
  BufferPoolManager *buffer_pool_;
};

}  // namespace bustub
