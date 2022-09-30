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
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, page_id_t next_page_id, int size, int cursor,
                                                                BufferPoolManager *buffer_pool) : page_id_(page_id), next_page_id_(next_page_id), size_(size), cursor_(cursor), buffer_pool_(buffer_pool) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (cursor_ + 1 == size_ && next_page_id_ == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto result_page = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(
      buffer_pool_ ->FetchPage(page_id_));
  const MappingType& result = result_page ->GetItem(cursor_);
  buffer_pool_ ->UnpinPage(result_page ->GetPageId(),false);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ++cursor_;
  if (cursor_ < size_) {
    return *this;
  }
  auto now_page = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(
      buffer_pool_ ->FetchPage(page_id_));
  auto next_page = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(
      buffer_pool_ ->FetchPage(now_page ->GetNextPageId()));
  buffer_pool_ ->UnpinPage(now_page ->GetPageId(), false);
  cursor_ = 0;
  size_ = next_page ->GetSize();
  page_id_ = next_page ->GetPageId();
  next_page_id_ = next_page ->GetNextPageId();
  buffer_pool_ ->UnpinPage(next_page ->GetPageId(), false);
  return *this;
}


template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
