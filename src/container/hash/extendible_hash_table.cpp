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
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  directory_page -> IncrGlobalDepth();
  page_id_t bucket_page_id_1 = INVALID_PAGE_ID;
  page_id_t bucket_page_id_2 = INVALID_PAGE_ID;
  buffer_pool_manager_ ->NewPage(&bucket_page_id_1, nullptr);
  buffer_pool_manager_ ->NewPage(&bucket_page_id_2, nullptr);
  directory_page->SetLocalDepth(0, 1);
  directory_page->SetLocalDepth(1, 1);
  directory_page->SetBucketPageId(0, bucket_page_id_1);
  directory_page->SetBucketPageId(1, bucket_page_id_2);
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
  auto global_mask = dir_page -> GetGlobalDepthMask();
  auto hash_key = (Hash(key) & global_mask);

  return hash_key;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline auto HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) -> uint32_t {
  auto directory_index = KeyToDirectoryIndex(key,dir_page);

  return dir_page ->GetBucketPageId(directory_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchDirectoryPage() -> HashTableDirectoryPage * {
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_ ->FetchPage(directory_page_id_));

  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) -> HASH_TABLE_BUCKET_TYPE * {
  auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_ ->FetchPage(bucket_page_id));

  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key,directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  //如果要插入的bucketpage中已经有完全相同的key-value对了，不插入
  if (bucket_page->FindElement(key,value,comparator_)) {
    buffer_pool_manager_ ->UnpinPage(directory_page_id_,false);
    buffer_pool_manager_ ->UnpinPage(bucket_page_id,false);
    return false;
  }
  auto directory_index = KeyToDirectoryIndex(key,directory_page);
  auto limit_size = (1 << ((directory_page ->GetLocalDepth(directory_index)) - 1));
  if (limit_size == static_cast<int>(bucket_page -> GetSize())) {
    //如果满了，要进行分裂
    auto last_local_depth = directory_page ->GetLocalDepth(directory_index);
    auto new_bucket_page_id =
        SplitBucketPage(bucket_page, directory_page, bucket_page_id, KeyToDirectoryIndex(key, directory_page));
    if (directory_page ->GetGlobalDepth() == last_local_depth) {
      directory_page ->IncrGlobalDepth();
    }
    UpdateDirectoryPage(directory_page,bucket_page_id,new_bucket_page_id);
    buffer_pool_manager_ ->UnpinPage(directory_page_id_,true);
    buffer_pool_manager_ ->UnpinPage(bucket_page_id,true);
  } else {
    bucket_page ->Insert(key,value,comparator_);
    buffer_pool_manager_ ->UnpinPage(directory_page_id_,false);
    buffer_pool_manager_ ->UnpinPage(bucket_page_id,true);
  }
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
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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
void ExtendibleHashTable<KeyType, ValueType, KeyComparator>::UpdateDirectoryPage(HashTableDirectoryPage *directory_page,
                                                                                 page_id_t id0, page_id_t id1) {
  //TODO
}
template <typename KeyType, typename ValueType, typename KeyComparator>
auto ExtendibleHashTable<KeyType, ValueType, KeyComparator>::SplitBucketPage(
    HashTableBucketPage<KeyType, ValueType, KeyComparator> *bucket_page, HashTableDirectoryPage *directory_page,
    page_id_t bucket_page_id, uint32_t bucket_index) -> page_id_t {
  //TODO
  directory_page ->IncrLocalDepth(bucket_index);
  auto local_depth = directory_page ->GetLocalDepth(bucket_index);
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page = reinterpret_cast<HashTableBucketPage<int, int, IntComparator> *>(
      buffer_pool_manager_->NewPage(&new_bucket_page_id, nullptr)->GetData());
  std::vector<MappingType> elements;
  bucket_page ->GetAllPairs(&elements);
  bucket_page ->clear();
  for (auto it : elements) {
    auto hash_val = (Hash(it.first) & directory_page -> GetGlobalDepth());
    auto check_bit = ((hash_val >> (local_depth - 1)) & 1);
    if (check_bit) {
      new_bucket_page ->Insert(it.first,it.second,comparator_);
    } else {
      bucket_page ->Insert(it.first,it.second,comparator_);
    }
  }

  return new_bucket_page_id;
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
