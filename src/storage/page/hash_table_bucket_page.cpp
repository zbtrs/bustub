//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool flag = false;
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0) {
      result->push_back(array_[i].second);
      flag = true;
    }
  }
  return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::GetAllPairs(
    std::vector<std::pair<KeyType, ValueType>> *vec) {
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i)) {
      vec->push_back(array_[i]);
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HashTableBucketPage<KeyType, ValueType, KeyComparator>::GetSize() {
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  int tot = 0;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i)) {
      tot++;
    }
  }
  return tot;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HashTableBucketPage<KeyType, ValueType, KeyComparator>::IsEmpty() -> bool {
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i)) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HashTableBucketPage<KeyType, ValueType, KeyComparator>::FindElement(KeyType key, ValueType value,
                                                                         KeyComparator cmp) -> bool {
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0 && value == array_[i].second) {
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  // 如果找到了完全相同的，就返回false
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  int lastpos = -1;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0 && value == array_[i].second) {
      return false;
    }
    if (IsOccupied(i)) {
      lastpos = i;
    }
  }
  lastpos++;
  std::pair<KeyType, ValueType> temp = std::make_pair(key, value);
  array_[lastpos] = temp;
  SetOccupied(lastpos);
  SetReadable(lastpos);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  int pos = -1;
  int size = ((BUCKET_ARRAY_SIZE - 1) / 8 + 1) * 8;
  for (int i = 0; i < size; ++i) {
    if (IsReadable(i) && cmp(key, array_[i].first) == 0 && value == array_[i].second) {
      pos = i;
      break;
    }
  }
  if (pos == -1) {
    return false;
  }
  RemoveReadable(pos);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  auto size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  if (bucket_idx / 8 >= size) {
    return false;
  }
  char temp = occupied_[bucket_idx / 8];
  auto num = (temp >> (bucket_idx - (bucket_idx / 8) * 8)) & 1;
  return num != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  int t = (1 << (bucket_idx - (bucket_idx / 8) * 8));
  occupied_[bucket_idx / 8] |= t;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveReadable(uint32_t bucket_idx) {
  int t = (1 << (bucket_idx - (bucket_idx / 8) * 8));
  readable_[bucket_idx / 8] ^= t;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  auto size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  if (bucket_idx / 8 >= size) {
    return false;
  }
  char temp = readable_[bucket_idx / 8];
  auto num = (temp >> (bucket_idx - (bucket_idx / 8) * 8)) & 1;
  return num != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  int t = (1 << (bucket_idx - (bucket_idx / 8) * 8));
  readable_[bucket_idx / 8] |= t;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  return 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HashTableBucketPage<KeyType, ValueType, KeyComparator>::Clear() {
  memset(occupied_, 0, sizeof(occupied_));
  memset(readable_, 0, sizeof(readable_));
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
