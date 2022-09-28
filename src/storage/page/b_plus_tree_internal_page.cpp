//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  max_size_ = max_size;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < size_; ++i) {
    if (value == array_[i].second) {
      return i;
    }
  }
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  if (size_ == 1) {
    return array_[0].second;
  }
  int l = 1;
  int r = size_ - 1;
  int res = 0;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(array_[mid].first, key) <= 0) {
      res = mid;
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return array_[res].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  size_ = 2;
  array_[0].second = old_value;
  array_[1] = std::make_pair(new_key, new_value);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) -> int {
  return 0;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) -> KeyType {
  auto index = size_ / 2;
  recipient->SetSize(size_ - index);
  recipient->SetItem(0, std::make_pair(array_[index].first, array_[index].second));
  for (int i = index + 1; i < size_; ++i) {
    recipient->SetItem(i - index, std::make_pair(array_[i].first, array_[i].second));
  }
  buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  size_ = index;

  return array_[index].first;
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  for (int i = size_ - 1; i >= 0; i--) {
    array_[i + size] = array_[i];
  }
  for (int i = 0; i < size; ++i) {
    array_[i] = items[i];
  }
  size_ += size;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i + 1 < size_; ++i) {
    array_[i] = array_[i + 1];
  }
  size_--;
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() -> ValueType { return INVALID_PAGE_ID; }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::MoveAllTo(BPlusTreeInternalPage *recipient,
                                                                         const KeyType &middle_key, int opt) {
  if (opt == 0) {
    recipient->CopyLastFrom(std::make_pair(middle_key, array_[0].second));
    for (int i = 1; i < size_; ++i) {
      recipient->CopyLastFrom(array_[i]);
    }
  } else {
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyNFrom(array_, size_);
  }
  size_ = 0;
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient) {
  auto element = array_[0];
  recipient->CopyLastFrom(element);
  for (int i = 0; i + 1 < size_; ++i) {
    array_[i] = array_[i + 1];
  }
  size_--;
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair) {
  array_[size_] = pair;
  size_++;
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient) {
  auto element = array_[size_ - 1];
  recipient->CopyFirstFrom(element);
  size_--;
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair) {
  for (int i = size_; i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = pair;
  size_++;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::Insert(const KeyType &key, const ValueType &value,
                                                                      const KeyComparator &comparator) -> int {
  if (size_ <= 1 || comparator(array_[size_ - 1].first, key) < 0) {
    array_[size_] = std::make_pair(key, value);
  } else {
    int l = 1;
    int r = size_ - 1;
    int res = r;
    while (l <= r) {
      int mid = (l + r) >> 1;
      if (comparator(array_[mid].first, key) >= 0) {
        res = mid;
        r = mid - 1;
      } else {
        l = mid + 1;
      }
    }
    for (int i = size_; i > res; i--) {
      array_[i] = array_[i - 1];
    }
    array_[res] = std::make_pair(key, value);
  }

  ++size_;
  return size_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::SetItem(int index, std::pair<KeyType, ValueType> item) {
  array_[index] = item;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::UpdateParentPageId(
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size_; ++i) {
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[i].second));
    child_page->SetParentPageId(page_id_);
    buffer_pool_manager->UnpinPage(array_[i].second, true);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::FindSiblings(KeyType key, KeyComparator comparator,
                                                                            page_id_t *left_sibling_page_id,
                                                                            page_id_t *right_sibling_page_id,
                                                                            int *index) {
  /*
  int l = 1;
  int r = size_ - 1;
  int res = r;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(array_[mid].first, key) >= 0) {
      res = mid;
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  */
  int l = 1;
  int r = size_ - 1;
  int res = 0;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(array_[mid].first,key) <= 0) {
      res = mid;
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  if (res >= 1) {
    *left_sibling_page_id = array_[res - 1].second;
  }
  if (res + 1 < size_) {
    *right_sibling_page_id = array_[res + 1].second;
  }
  *index = res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::UpdateNewParentId(
    page_id_t new_page_id, BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < static_cast<int>(size_); ++i) {
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(array_[i].second));
    child_page->SetParentPageId(new_page_id);
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
int BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>::LookupKey(const KeyType &key, const KeyComparator &comparator) {
  if (size_ == 1) {
    return 0;
  }
  int l = 1;
  int r = size_ - 1;
  int res = 0;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (comparator(array_[mid].first, key) <= 0) {
      res = mid;
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  return res;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
