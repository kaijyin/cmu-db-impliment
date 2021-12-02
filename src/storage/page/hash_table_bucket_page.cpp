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
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  uint32_t idx = 0;
  uint32_t max_size = BUCKET_ARRAY_SIZE;
  while (idx < max_size && IsOccupied(idx)) {
    if (IsReadable(idx)) {
      if (cmp.operator()(key, KeyAt(idx)) == 0) {
        result->push_back(ValueAt(idx));
      }
    }
    idx++;
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t tombstone_idx;
  bool hastombstone = false;
  size_t idx = 0;
  while (IsOccupied(idx)) {
    if (IsReadable(idx)) {
      if (cmp.operator()(key, KeyAt(idx)) == 0 && ValueAt(idx) == value) {
        return false;
      }
    } else {
      hastombstone = true;
      tombstone_idx = idx;
    }
    idx++;
  }
  // 除非key,val重复,不然一定能插入成功,因为到达maxsize就会分裂
  if (hastombstone) {
    ChangeReadable(tombstone_idx);
    array_[tombstone_idx] = MappingType(key, value);
  } else {
    ChangeReadable(idx);
    ChangeOccupied(idx);
    array_[idx] = MappingType(key, value);
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  uint32_t idx = 0;
  uint32_t max_size = BUCKET_ARRAY_SIZE;
  while (idx < max_size && IsOccupied(idx)) {
    if (IsReadable(idx)) {
      if (cmp.operator()(key, KeyAt(idx)) == 0 && value == ValueAt(idx)) {
        RemoveAt(idx);
        return true;
      }
    }
    idx++;
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  // 应该不会删除不存在的索引
  ChangeReadable(bucket_idx);
  if ((bucket_idx == (BUCKET_ARRAY_SIZE - 1)) || (!IsReadable(bucket_idx + 1) && !IsOccupied(bucket_idx + 1))) {
    while (bucket_idx >= 0) {
      if (!IsReadable(bucket_idx) && IsOccupied(bucket_idx)) {
        ChangeOccupied(bucket_idx);
      } else {
        break;
      }
      if (bucket_idx == 0) {
        break;
      }
      bucket_idx--;
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return occupied_[bucket_idx / 8] & (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ChangeOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx / 8] ^= (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return readable_[bucket_idx / 8] & (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::ChangeReadable(uint32_t bucket_idx) {
  readable_[bucket_idx / 8] ^= (1 << (bucket_idx % 8));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  size_t size = (BUCKET_ARRAY_SIZE - 1) / 8 + 1;
  uint32_t sum = 0;
  uint8_t mask = 255;
  for (size_t now_idx = 0; now_idx < size; now_idx++) {
    int now_x = static_cast<int>(readable_[now_idx]);
    while (now_x != 0) {
      now_x -= (now_x & (-now_x));
      sum++;
    }
    if ((occupied_[now_idx] ^ mask) != 0) {
      return sum;
    }
  }
  return sum;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  size_t size = (BUCKET_ARRAY_SIZE - 1) / 8;
  // 倒着往前判断
  uint8_t mask = (1 << (BUCKET_ARRAY_SIZE - 8 * size)) - 1;
  if ((readable_[size] ^ mask) != 0) {
    return false;
  }
  mask = 255;
  for (size_t i = size - 1; i >= 0; i--) {
    if ((readable_[i] ^ mask) != 0) {
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return !IsOccupied(0);
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

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
