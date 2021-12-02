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
  //  implement me!
  Page *dir_page = NewPage(&directory_page_id_);
  HashTableDirectoryPage *dir_node = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  dir_node->SetPageId(directory_page_id_);
  page_id_t bucket_page_id;
  Page *bucket_page = NewPage(&bucket_page_id);
  dir_node->SetBucketPageId(0, bucket_page_id);
  UnpinPage(bucket_page, LockMode::WRITE);
  UnpinPage(dir_page, LockMode::WRITE, true);
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
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::NewPage(page_id_t *page_id) {
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "new page error");
  }
  page->WLatch();
  return page;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
Page *HASH_TABLE_TYPE::FetchPage(page_id_t page_id, LockMode lock_mode) {
  auto page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch error");
  }
  if (lock_mode == LockMode::READ) {
    page->RLatch();
  } else if (lock_mode == LockMode::WRITE) {
    page->WLatch();
  }
  return page;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::UnpinPage(Page *page, LockMode lock_mode, bool dirty) {
  if (lock_mode == LockMode::READ) {
    page->RUnlatch();
  } else if (lock_mode == LockMode::WRITE) {
    page->WUnlatch();
  }
  return buffer_pool_manager_->UnpinPage(page->GetPageId(), dirty);
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto dir_page = FetchPage(directory_page_id_, LockMode::READ);
  auto dir = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  auto bucket_idx = Hash(key) & dir->GetGlobalDepthMask();
  auto bucket_page = FetchPage(dir->GetBucketPageId(bucket_idx), LockMode::READ);
  UnpinPage(dir_page, LockMode::READ);

  HASH_TABLE_BUCKET_TYPE *buk = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  bool res = buk->GetValue(key, comparator_, result);
  UnpinPage(bucket_page, LockMode::READ, false);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchPage(directory_page_id_, LockMode::READ);
  auto dir_node = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  auto bucket_idx = Hash(key) & dir_node->GetGlobalDepthMask();
  auto bucket_page = FetchPage(dir_node->GetBucketPageId(bucket_idx), LockMode::WRITE);
  HASH_TABLE_BUCKET_TYPE *buk_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  if (buk_node->IsFull()) {
    UnpinPage(bucket_page, LockMode::WRITE, false);
    UnpinPage(dir_page, LockMode::READ);
    table_latch_.RUnlock();
    return SplitInsert(transaction, key, value);
  }
  bool res = buk_node->Insert(key, value, comparator_);
  UnpinPage(dir_page, LockMode::READ);
  UnpinPage(bucket_page, LockMode::WRITE, true);
  table_latch_.RUnlock();
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchPage(directory_page_id_, LockMode::WRITE);
  auto dir_node = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  auto bucket_idx = Hash(key) & dir_node->GetGlobalDepthMask();
  auto bucket_page = FetchPage(dir_node->GetBucketPageId(bucket_idx), LockMode::WRITE);
  HASH_TABLE_BUCKET_TYPE *buk_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  bool res;
  while (buk_node->IsFull()) {
    page_id_t sib_page_id;
    auto sib_page = NewPage(&sib_page_id);
    HASH_TABLE_BUCKET_TYPE *sib_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(sib_page->GetData());
    uint32_t low_idx = dir_node->GetLowMatch(bucket_idx);
    uint32_t add_bit = dir_node->GetLocalDepthMask(bucket_idx) + 1;
    if (dir_node->GetGlobalDepth() == dir_node->GetLocalDepth(bucket_idx)) {
      dir_node->IncrGlobalDepth();
    }
    uint32_t global_size = dir_node->Size();
    bool f = false;
    while (low_idx < global_size) {
      dir_node->IncrLocalDepth(low_idx);
      if (f) {
        dir_node->SetBucketPageId(low_idx, sib_page->GetPageId());
      }
      low_idx += add_bit;
      f = !f;
    }
    uint32_t array_size = BUCKET_ARRAY_SIZE;
    for (uint32_t i = 0; i < array_size; i++) {
      KeyType cur_key = buk_node->KeyAt(i);
      uint32_t hash_idx = Hash(cur_key);
      // 高位为1的替换到分到另一页
      if ((dir_node->GetLocalHighBit(hash_idx) ^ hash_idx) == 0) {
        buk_node->RemoveAt(i);
        sib_node->Insert(cur_key, buk_node->ValueAt(i), comparator_);
      }
    }
    UnpinPage(bucket_page, LockMode::WRITE, true);
    UnpinPage(sib_page, LockMode::WRITE, true);
    bucket_idx = Hash(key) & dir_node->GetGlobalDepthMask();
    bucket_page = FetchPage(dir_node->GetBucketPageId(bucket_idx), LockMode::WRITE);
    buk_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  }
  res = buk_node->Insert(key, value, comparator_);
  UnpinPage(bucket_page, LockMode::WRITE, true);
  UnpinPage(dir_page, LockMode::WRITE, true);
  table_latch_.WUnlock();
  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchPage(directory_page_id_, LockMode::READ);
  auto dir_node = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  auto bucket_idx = Hash(key) & dir_node->GetGlobalDepthMask();
  auto bucket_page = FetchPage(dir_node->GetBucketPageId(bucket_idx), LockMode::WRITE);
  HASH_TABLE_BUCKET_TYPE *buk_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  bool res = buk_node->Remove(key, value, comparator_);
  bool empty = buk_node->IsEmpty();
  UnpinPage(dir_page, LockMode::READ);
  UnpinPage(bucket_page, LockMode::WRITE, true);
  table_latch_.RUnlock();
  if (empty) {
    Merge(transaction, key, value);
  }
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchPage(directory_page_id_, LockMode::WRITE);
  auto dir_node = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  auto bucket_idx = Hash(key) & dir_node->GetGlobalDepthMask();
  auto bucket_page = FetchPage(dir_node->GetBucketPageId(bucket_idx), LockMode::WRITE);
  HASH_TABLE_BUCKET_TYPE *buk_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  uint32_t sib_idx = dir_node->GetSpliteImageIdx(bucket_idx);
  while (buk_node->IsEmpty() && (dir_node->GetLocalDepth(bucket_idx) > 0) &&
         (dir_node->GetLocalDepth(sib_idx) == dir_node->GetLocalDepth(bucket_idx))) {
    page_id_t sib_page_id = dir_node->GetBucketPageId(sib_idx);
    uint32_t low_idx = dir_node->GetLowMatch(bucket_idx);
    uint32_t add_bit = dir_node->GetLocalDepthMask(bucket_idx) + 1;
    if (dir_node->GetGlobalDepth() == dir_node->GetLocalDepth(bucket_idx)) {
      uint32_t mask = dir_node->GetLocalDepthMask(bucket_idx);
      uint32_t now_size = dir_node->Size();
      bool the_match_dep = true;
      for (uint32_t idx = 0; idx < now_size; idx++) {
        if ((dir_node->GetLocalDepth(idx) == dir_node->GetGlobalDepth()) && ((idx & mask) != low_idx)) {
          the_match_dep = false;
          break;
        }
      }
      if (the_match_dep) {
        dir_node->IncrGlobalDepth();
      }
    }
    uint32_t global_size = dir_node->Size();
    while (low_idx < global_size) {
      dir_node->DecrLocalDepth(low_idx);
      dir_node->SetBucketPageId(low_idx, sib_page_id);
      low_idx += add_bit;
    }
    sib_idx = dir_node->GetSpliteImageIdx(bucket_idx);
    UnpinPage(bucket_page, LockMode::WRITE, true);
    buffer_pool_manager_->DeletePage(bucket_page->GetPageId());
    bucket_page = FetchPage(dir_node->GetBucketPageId(bucket_idx), LockMode::WRITE);
    buk_node = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(bucket_page->GetData());
  }
  UnpinPage(dir_page, LockMode::WRITE, true);
  UnpinPage(bucket_page, LockMode::WRITE, true);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
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
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
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
