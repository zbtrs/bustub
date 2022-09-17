//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  directory_page->IncrGlobalDepth();
  page_id_t bucket_page_id_1 = INVALID_PAGE_ID;
  page_id_t bucket_page_id_2 = INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&bucket_page_id_1, nullptr);
  buffer_pool_manager_->NewPage(&bucket_page_id_2, nullptr);
  directory_page->SetLocalDepth(0, 1);
  directory_page->SetLocalDepth(1, 1);
  directory_page->SetBucketPageId(0, bucket_page_id_1);
  directory_page->SetBucketPageId(1, bucket_page_id_2);
  buffer_pool_manager_->UnpinPage(bucket_page_id_1, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id_2, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  auto global_mask = dir_page->GetGlobalDepthMask();
  auto hash_val = Hash(key);
  auto hash_key = (hash_val & global_mask);

  return hash_key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  auto directory_index = KeyToDirectoryIndex(key, dir_page);

  return dir_page->GetBucketPageId(directory_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));

  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));

  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  table_latch_.RLock();
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  AddBucketLatch(bucket_page, 'R');
  bool flag = bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  table_latch_.RUnlock();
  RemoveBucketLatch(bucket_page, 'R');

  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  auto directory_page = FetchDirectoryPage();
  AddDirectoryLatch(directory_page, 'R');
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  RemoveDirectoryLatch(directory_page, 'R');
  // 如果要插入的bucketpage中已经有完全相同的key-value了，不插入
  AddBucketLatch(bucket_page, 'R');
  if (bucket_page->FindElement(key, value, comparator_)) {
    RemoveBucketLatch(bucket_page, 'R');
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return false;
  }
  RemoveBucketLatch(bucket_page, 'R');
  AddDirectoryLatch(directory_page, 'W');
  AddBucketLatch(bucket_page, 'W');
  auto directory_index = KeyToDirectoryIndex(key, directory_page);
  auto limit_size = (1 << ((directory_page->GetLocalDepth(directory_index)) - 1));
  if (limit_size == static_cast<int>(bucket_page->GetSize())) {
    // 如果满了，要进行分裂
    auto last_local_depth = directory_page->GetLocalDepth(directory_index);
    auto new_bucket_page_id =
        SplitBucketPage(bucket_page, directory_page, bucket_page_id, KeyToDirectoryIndex(key, directory_page));
    if (directory_page->GetGlobalDepth() == last_local_depth) {
      directory_page->IncrGlobalDepth();
      UpdateDirectoryPage(directory_page, bucket_page_id, new_bucket_page_id);
    } else {
      UpdateLittleDirectoryPage(directory_page, bucket_page_id, new_bucket_page_id, last_local_depth + 1);
    }
    auto another_bucket_page_id = KeyToPageId(key, directory_page);
    if (another_bucket_page_id != bucket_page_id) {
      auto another_bucket_page = FetchBucketPage(another_bucket_page_id);
      AddBucketLatch(another_bucket_page, 'W');
      another_bucket_page->Insert(key, value, comparator_);
      buffer_pool_manager_->UnpinPage(another_bucket_page_id, true);
      RemoveBucketLatch(another_bucket_page, 'W');
    } else {
      bucket_page->Insert(key, value, comparator_);
    }
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  } else {
    bucket_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  }
  RemoveBucketLatch(bucket_page, 'W');
  RemoveDirectoryLatch(directory_page, 'W');
  table_latch_.WUnlock();

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  table_latch_.WLock();
  auto directory_page = FetchDirectoryPage();
  AddDirectoryLatch(directory_page, 'W');
  auto bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto directory_index = KeyToDirectoryIndex(key, directory_page);
  auto local_depth = directory_page->GetLocalDepth(directory_index);
  page_id_t another_bucket_page_id = 0;
  AddBucketLatch(bucket_page, 'W');
  if (!bucket_page->FindElement(key, value, comparator_) &&
      (!bucket_page->IsEmpty() || !CheckMerge(directory_page, directory_index, local_depth, &another_bucket_page_id))) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    RemoveDirectoryLatch(directory_page, 'W');
    RemoveBucketLatch(bucket_page, 'W');
    return false;
  }
  bucket_page->Remove(key, value, comparator_);
  if (!bucket_page->IsEmpty() || !CheckMerge(directory_page, directory_index, local_depth, &another_bucket_page_id)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    table_latch_.WUnlock();
    RemoveDirectoryLatch(directory_page, 'W');
    RemoveBucketLatch(bucket_page, 'W');
    return true;
  }
  // try to merge
  Merge(nullptr, directory_page, bucket_page_id, another_bucket_page_id);
  if (CheckUpdateGlobalDepth(directory_page)) {
    // update global depth
    directory_page->DecrGlobalDepth();
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.WUnlock();
  RemoveDirectoryLatch(directory_page, 'W');
  RemoveBucketLatch(bucket_page, 'W');

  return true;
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::UpdateLittleDirectoryPage(
    HashTableDirectoryPage *directory_page, page_id_t id0, page_id_t id1, uint32_t local_depth) {
  auto global_depth = directory_page->GetGlobalDepth();
  auto limit = (1 << global_depth);
  for (auto i = 0; i < limit; ++i) {
    if (directory_page->GetBucketPageId(i) == id0) {
      directory_page->SetLocalDepth(i, local_depth);
      auto maxbit = ((i >> (local_depth - 1)) & 1);
      if (maxbit != 0) {
        directory_page->SetBucketPageId(i, id1);
      }
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::UpdateDirectoryPage(HashTableDirectoryPage *directory_page,
                                                                                 page_id_t id0, page_id_t id1) {
  auto offset = (1 << (directory_page->GetGlobalDepth() - 1));
  auto global_depth = directory_page->GetGlobalDepth();
  // 先复制指针
  for (auto i = 0; i < offset; i++) {
    directory_page->SetBucketPageId(i + offset, directory_page->GetBucketPageId(i));
    directory_page->SetLocalDepth(i + offset, directory_page->GetLocalDepth(i));
  }
  offset <<= 1;
  for (auto i = 0; i < offset; i++) {
    if (directory_page->GetBucketPageId(i) == id0) {
      auto maxbit = ((i >> (global_depth - 1)) & 1);
      if (maxbit != 0) {
        directory_page->SetBucketPageId(i, id1);
      }
      directory_page->SetLocalDepth(i, global_depth);
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto ExtendibleHashTable<KeyType, ValueType, KeyComparator>::SplitBucketPage(
    HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page, HashTableDirectoryPage *directory_page,
    page_id_t bucket_page_id, uint32_t bucket_index) -> page_id_t {
  auto local_depth = directory_page->GetLocalDepth(bucket_index);
  local_depth++;
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_bucket_page_id, nullptr)->GetData());
  AddBucketLatch(new_bucket_page, 'W');
  new_bucket_page->Clear();
  std::vector<MappingType> elements;
  bucket_page->GetAllPairs(&elements);
  bucket_page->Clear();
  auto maskbits = (1 << local_depth) - 1;
  for (auto it : elements) {
    auto hash_val = (Hash(it.first) & maskbits);
    auto check_bit = ((hash_val >> (local_depth - 1)) & 1);
    if (check_bit) {
      new_bucket_page->Insert(it.first, it.second, comparator_);
    } else {
      bucket_page->Insert(it.first, it.second, comparator_);
    }
  }
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
  RemoveBucketLatch(new_bucket_page, 'W');

  return new_bucket_page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool ExtendibleHashTable<KeyType, ValueType, KeyComparator>::CheckMerge(HashTableDirectoryPage *directory_page,
                                                                        uint32_t pid, uint32_t local_depth,
                                                                        page_id_t *pInt) {
  auto global_depth = directory_page->GetGlobalDepth();
  auto maxbit = (1 << (global_depth - 1));
  auto npid = (pid ^ maxbit);
  if (local_depth <= 1 || directory_page->GetLocalDepth(npid) != local_depth) {
    return false;
  }
  auto bucket_page_id = directory_page->GetBucketPageId(npid);
  if (bucket_page_id == directory_page->GetBucketPageId(pid)) {
    return false;
  }
  *pInt = bucket_page_id;
  auto bucket_page = FetchBucketPage(bucket_page_id);
  AddBucketLatch(bucket_page, 'R');
  bool flag = bucket_page->IsEmpty();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  RemoveBucketLatch(bucket_page, 'R');

  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::Merge(Transaction *transaction,
                                                                   HashTableDirectoryPage *directory_page,
                                                                   page_id_t id0, page_id_t id1) {
  auto global_mask = directory_page->GetGlobalDepthMask();
  for (int i = 0; i <= static_cast<int>(global_mask); ++i) {
    auto bucket_page_id = directory_page->GetBucketPageId(i);
    if (bucket_page_id == id0) {
      directory_page->DecrLocalDepth(i);
    } else if (bucket_page_id == id1) {
      directory_page->DecrLocalDepth(i);
      directory_page->SetBucketPageId(i, id0);
    }
  }
  buffer_pool_manager_->UnpinPage(id1, true);
  buffer_pool_manager_->DeletePage(id1);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool ExtendibleHashTable<KeyType, ValueType, KeyComparator>::CheckUpdateGlobalDepth(
    HashTableDirectoryPage *directory_page) {
  auto global_mask = directory_page->GetGlobalDepthMask();
  auto global_depth = directory_page->GetGlobalDepth();
  for (int i = 0; i <= static_cast<int>(global_mask); ++i) {
    if (directory_page->GetLocalDepth(i) >= global_depth) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AddDirectoryLatch(HashTableDirectoryPage *directory_page,
                                                                               char ch) {
  if (ch == 'R') {
    reinterpret_cast<Page *>(directory_page)->RLatch();
  } else {
    reinterpret_cast<Page *>(directory_page)->WLatch();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::RemoveDirectoryLatch(
    HashTableDirectoryPage *directory_page, char ch) {
  if (ch == 'R') {
    reinterpret_cast<Page *>(directory_page)->RUnlatch();
  } else {
    reinterpret_cast<Page *>(directory_page)->WUnlatch();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::AddBucketLatch(
    HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page, char ch) {
  if (ch == 'R') {
    reinterpret_cast<Page *>(bucket_page)->RLatch();
  } else {
    reinterpret_cast<Page *>(bucket_page)->WLatch();
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::RemoveBucketLatch(
    HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page, char ch) {
  if (ch == 'R') {
    reinterpret_cast<Page *>(bucket_page)->RUnlatch();
  } else {
    reinterpret_cast<Page *>(bucket_page)->WUnlatch();
  }
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
