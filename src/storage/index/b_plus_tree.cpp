//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size){UpdateRootPageId();}

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
  auto leaf_page = FindLeafPage(key);
  ValueType res;
  bool flag = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page)->Lookup(key, &res,
                                                                                                          comparator_);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  if (flag) {
    result->push_back(res);
    return true;
  }
  return false;
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
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  auto leaf_page = FindLeafPage(key);
  bool flag = InsertIntoLeaf(leaf_page, key, value, transaction);

  return flag;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto new_root_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&root_page_id_, nullptr)->GetData());
  new_root_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  new_root_page->SetPageType(IndexPageType::LEAF_PAGE);
  new_root_page->SetNextPageId(INVALID_PAGE_ID);
  UpdateRootPageId();
  InsertIntoLeaf(new_root_page, key, value);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(BPlusTreePage *node, const KeyType &key, const ValueType &value,
                                    Transaction *transaction) -> bool {
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
  ValueType exist_value;
  if (leaf_page->Lookup(key, &exist_value, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  auto leaf_page_size = leaf_page->Insert(key, value, comparator_);
  if (leaf_page_size < leaf_max_size_) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  auto new_leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(Split(leaf_page));
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page->GetPageId());
  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);

  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Split(BPlusTreePage *node) -> BPlusTreePage * {
  if (node->IsLeafPage()) {
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->NewPage(&new_page_id, nullptr)->GetData());
    new_page->Init(new_page_id, node->GetParentPageId(), leaf_max_size_);
    new_page->SetPageType(IndexPageType::LEAF_PAGE);
    reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node)->MoveHalfTo(new_page);
    KeyType key = new_page->KeyAt(0);
    InsertIntoParent(node, key, new_page_id, nullptr);

    return new_page;
  }
  page_id_t new_page_id;
  auto new_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_page_id, nullptr)->GetData());
  new_page->Init(new_page_id, node->GetParentPageId(), internal_max_size_);
  new_page->SetPageType(IndexPageType::INTERNAL_PAGE);
  auto key = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node)->MoveHalfTo(
      new_page, buffer_pool_manager_);
  new_page->UpdateParentPageId(buffer_pool_manager_);
  InsertIntoParent(node, key, new_page_id, nullptr);

  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, page_id_t new_page_id,
                                      Transaction *transaction) {
  if (old_node->GetPageId() == root_page_id_) {
    auto new_root_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        buffer_pool_manager_->NewPage(&root_page_id_, nullptr)->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root_page->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_page_id);
    UpdateRootPageId();
    old_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    auto new_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(new_page_id));
    new_page->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    if (!old_node->IsLeafPage()) {
      buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    }
    return;
  }
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  if (!old_node->IsLeafPage()) {
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  }
  auto parent_page_size = parent_page->Insert(key, new_page_id, comparator_);
  if (parent_page_size >= internal_max_size_) {
    auto new_page = Split(parent_page);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  } else {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
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
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(FindLeafPage(key));
  auto index = leaf_page->KeyIndex(key, comparator_);
  if (comparator_(leaf_page ->KeyAt(index),key) != 0) {
    return;
  }
  bool delete_min = false;
  if (index == 0 && comparator_(leaf_page->KeyAt(index), key) == 0) {
    delete_min = true;
  }
  KeyType min_key = leaf_page->KeyAt(0);
  auto leaf_page_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);


  if (leaf_page_size >= leaf_page->GetMinSize()) {
    if (delete_min && leaf_page->GetPageId() != root_page_id_ && leaf_page_size > 0) {
      auto parent_page =
          reinterpret_cast<BPlusTreeInternalPage<KeyType,
                                                 page_id_t , KeyComparator> *>(
              buffer_pool_manager_ ->FetchPage(leaf_page->GetParentPageId()));
      // TODO:why problem
      int key_index = parent_page ->LookupKey(min_key,comparator_);
      auto last_key = parent_page ->KeyAt(key_index);
      parent_page ->SetKeyAt(key_index, leaf_page->KeyAt(0));
      if (key_index == 1) {
        RecursiveUpdate(last_key, parent_page ->KeyAt(1),parent_page ->GetParentPageId());
      }
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }

  CoalesceOrRedistribute(leaf_page, min_key, transaction);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, KeyType min_key, Transaction *transaction) -> bool {
  auto node_page = reinterpret_cast<BPlusTreePage *>(node);
  if (node_page->GetPageId() == root_page_id_) {
    return AdjustRoot(node_page);
  }
  page_id_t left_sibling_page_id = INVALID_PAGE_ID;
  page_id_t right_sibling_page_id = INVALID_PAGE_ID;
  int index = 0;
  auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(node_page->GetParentPageId()));
  parent_page->FindSiblings(min_key, comparator_, &left_sibling_page_id, &right_sibling_page_id, &index);
  if (left_sibling_page_id == INVALID_PAGE_ID) {
    // deal with right_sibling
    auto right_sibling_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_sibling_page_id));
    if (right_sibling_page->GetSize() >= right_sibling_page->GetMinSize() + 1) {
      // redistribute
      Redistribute(right_sibling_page, node_page, parent_page, 1, index);
    } else {
      Coalesce(right_sibling_page, node_page, parent_page, 1, index, transaction);
    }

  } else if (right_sibling_page_id == INVALID_PAGE_ID) {
    // deal with left_sibling
    auto left_sibling_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_sibling_page_id));
    if (left_sibling_page->GetSize() >= left_sibling_page->GetMinSize() + 1) {
      Redistribute(left_sibling_page, node_page, parent_page, 0, index);
    } else {
      Coalesce(left_sibling_page, node_page, parent_page, 0, index, transaction);
    }
  } else {
    auto left_sibling_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(left_sibling_page_id));
    if (left_sibling_page->GetSize() >= left_sibling_page->GetMinSize() + 1) {
      Redistribute(left_sibling_page, node_page, parent_page, 0, index);
    } else {
      auto right_sibling_page =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(right_sibling_page_id));
      if (right_sibling_page->GetSize() >= right_sibling_page->GetMinSize() + 1) {
        // redistribute
        buffer_pool_manager_->UnpinPage(left_sibling_page_id, false);
        Redistribute(right_sibling_page, node_page, parent_page, 1, index);
      } else {
        buffer_pool_manager_->UnpinPage(right_sibling_page_id, false);
        Coalesce(left_sibling_page, node_page, parent_page, 0, index, transaction);
      }
    }
  }

  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int opt, int index,
                              Transaction *transaction) -> bool {
  // opt == 0: coalesce node with his left neighbor; opt == 1: coalesce node with his right neighbor
  if (node->IsLeafPage()) {
    auto node_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    auto neighbor_node_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(neighbor_node);
    node_page->MoveAllTo(neighbor_node_page, opt);
    if (opt == 0) {
      neighbor_node_page->SetNextPageId(node_page->GetNextPageId());
    } else if (index > 0) {
      auto left_node_page_id = parent->ValueAt(index - 1);
      auto left_node_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          buffer_pool_manager_->FetchPage(left_node_page_id));
      left_node_page->SetNextPageId(neighbor_node_page->GetPageId());
      buffer_pool_manager_->UnpinPage(left_node_page_id, true);
    }
    RemoveParent(parent, index);
    auto node_page_id = node_page->GetPageId();
    buffer_pool_manager_->UnpinPage(node_page_id, true);
    buffer_pool_manager_->DeletePage(node_page_id);
    buffer_pool_manager_->UnpinPage(neighbor_node_page->GetPageId(), true);
  } else {
    // coalesce internal node
    auto node_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    auto neighbor_node_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);
    auto node_page_id = node_page->GetPageId();
    node_page->UpdateNewParentId(neighbor_node_page->GetPageId(), buffer_pool_manager_);
    if (opt == 0) {
      node_page->MoveAllTo(neighbor_node_page, parent->KeyAt(index), opt);
      RemoveParent(parent, index);
    } else {
      node_page->MoveAllTo(neighbor_node_page, parent->KeyAt(index + 1), opt);
      parent ->SetValueAt(index,neighbor_node_page ->GetPageId());
      RemoveParent(parent, index + 1);
    }
    // TODO:fix it
    buffer_pool_manager_->UnpinPage(node_page_id, true);
    buffer_pool_manager_->DeletePage(node_page_id);
    buffer_pool_manager_->UnpinPage(neighbor_node_page->GetPageId(), true);
  }

  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page, int opt,
                                  int index) {
  // opt == 0:redistribute with left sibling; opt == 1:redistribute with right sibling

  if (neighbor_node->IsLeafPage()) {
    auto neighbor_node_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(neighbor_node);
    auto node_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    if (opt == 0) {
      neighbor_node_page->MoveLastToFrontOf(node_page);
      bool is_change = comparator_(neighbor_node_page ->KeyAt(0),parent_page ->KeyAt(index - 1)) != 0;
      auto last_key = parent_page ->KeyAt(index - 1);
      parent_page->SetKeyAt(index - 1, neighbor_node_page->KeyAt(0));
      if (index - 1 == 0 && is_change) {
        RecursiveUpdate(last_key, parent_page->KeyAt(0), parent_page->GetParentPageId());
      }
    } else {
      neighbor_node_page->MoveFirstToEndOf(node_page);
      parent_page->SetKeyAt(index + 1, neighbor_node_page->KeyAt(0));
    }
    bool is_change = comparator_(node_page ->KeyAt(0), parent_page ->KeyAt(index)) != 0;
    auto last_key = parent_page ->KeyAt(index);
    parent_page->SetKeyAt(index, node_page->KeyAt(0));
    if (index == 0 && is_change) {
      RecursiveUpdate(last_key, parent_page->KeyAt(0), parent_page->GetParentPageId());
    }
  } else {
    // internal node
    auto neighbor_node_page =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(neighbor_node);
    auto node_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    if (opt == 1) {
      index++;
    }
    auto parent_key = parent_page->KeyAt(index);
    auto last_key = parent_page ->KeyAt(index);
    bool is_change = false;
    if (opt == 1) {
      auto neighbor_key = neighbor_node_page->KeyAt(1);
      neighbor_node_page->MoveFirstToEndOf(node_page);
      node_page->SetKeyAt(node_page->GetSize() - 1, parent_key);
      if (comparator_(last_key,neighbor_key) != 0) {
        is_change = true;
      }
      parent_page->SetKeyAt(index, neighbor_key);
    } else {
      auto neighbor_key = neighbor_node_page->KeyAt(neighbor_node_page->GetSize() - 1);
      neighbor_node_page->MoveLastToFrontOf(node_page);
      node_page->SetKeyAt(1, parent_key);
      if (comparator_(last_key,neighbor_key) != 0) {
        is_change = true;
      }
      parent_page->SetKeyAt(index, neighbor_key);
    }

    if (index == 1 && is_change) {
      RecursiveUpdate(last_key, parent_page->KeyAt(1), parent_page->GetParentPageId());
    }
  }
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) -> bool {
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      // delete all the tree
      auto old_root_node_id = old_root_node->GetPageId();
      buffer_pool_manager_->UnpinPage(old_root_node_id, true);
      buffer_pool_manager_->DeletePage(old_root_node_id);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
      return true;
    }
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    return false;
  }
  if (old_root_node->GetSize() > 1) {
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    return false;
  }
  auto new_root_page_id =
      reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node)->ValueAt(0);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  buffer_pool_manager_->DeletePage(root_page_id_);
  root_page_id_ = new_root_page_id;
  UpdateRootPageId();

  return true;
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

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) -> BPlusTreePage * {
  auto find_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  while (!find_page->IsLeafPage()) {
    auto last_page_id = find_page->GetPageId();
    auto next_find_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(find_page);
    find_page =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(next_find_page->Lookup(key, comparator_)));

    buffer_pool_manager_->UnpinPage(last_page_id, false);
  }
  return find_page;
}

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

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::RecursiveUpdate(KeyType min_key, const KeyType &key, page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) {
    return;
  }
  auto update_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(page_id));
  auto index = update_page ->LookupKey(min_key,comparator_);
  min_key = update_page ->KeyAt(index);
  update_page ->SetKeyAt(index, key);

  while (index == 1 && update_page ->GetPageId() != root_page_id_) {
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(update_page->GetParentPageId()));
    if (parent_page ->ValueAt(1) == update_page ->GetPageId()) {
      index = 1;
    } else {
      index = parent_page->LookupKey(min_key, comparator_);
    }
    min_key = parent_page ->KeyAt(index);
    parent_page ->SetKeyAt(index,key);
    buffer_pool_manager_ ->UnpinPage(update_page ->GetPageId(),true);
    update_page = parent_page;
    /*
    if (parent_page->ValueAt(1) != update_page->GetPageId()) {
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(update_page->GetPageId(), true);
      return;
    }
    parent_page->SetKeyAt(1, update_page->KeyAt(1));
    buffer_pool_manager_->UnpinPage(update_page->GetPageId(), true);
    update_page = parent_page;
     */
  }
  buffer_pool_manager_->UnpinPage(update_page->GetPageId(), true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void BPlusTree<KeyType, ValueType, KeyComparator>::RemoveParent(
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_page, int index) {
  auto min_key = parent_page->KeyAt(1);
  /*
  if (index == 1) {
    RecursiveUpdate(min_key,parent_page ->KeyAt(1),parent_page ->GetParentPageId());
  }
   */
  parent_page->Remove(index);
  if (parent_page->GetSize() >= parent_page->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  CoalesceOrRedistribute(parent_page, min_key);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
