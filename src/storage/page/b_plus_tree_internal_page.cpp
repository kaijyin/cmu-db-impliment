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

#include "include/common/logger.h"
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
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(int l, int r, const KeyType &key,
                                               const KeyComparator &comparator) const {
  int left = l;
  int right = r - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator.operator()(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::UpperBound(int l, int r, const KeyType &key,
                                               const KeyComparator &comparator) const {
  int left = l;
  int right = r - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (comparator.operator()(array_[mid].first, key) <= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return left;
}
/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    if (value == array_[i].second) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const { return array_[index].second; }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  int size = GetSize();
  int index = UpperBound(1, size, key, comparator) - 1;
  return array_[index].second;
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
  array_[0].second = old_value;
  array_[1].first = new_key;
  array_[1].second = new_value;
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int index = ValueIndex(old_value) + 1;
  int size = GetSize();
  for (int i = size; i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::make_pair(new_key, new_value);
  IncreaseSize(1);
  return size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  int old_size = GetSize();
  int new_size = (old_size + 1) / 2;
  recipient->CopyNFrom(&array_[new_size], old_size - new_size, buffer_pool_manager);
  SetSize(new_size);
}

INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *B_PLUS_TREE_INTERNAL_PAGE_TYPE::FetchBPlusTreePage(page_id_t page_id,
                                                                  BufferPoolManager *buffer_pool_manager) {
  Page *page = buffer_pool_manager->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch error");
  }
  return reinterpret_cast<BPlusTreePage *>(page->GetData());
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  int pre_size = GetSize();
  memcpy(reinterpret_cast<void *>(&array_[pre_size]), reinterpret_cast<void *>(items), size * (sizeof(MappingType)));
  IncreaseSize(size);
  int new_size = GetSize();
  for (int i = pre_size; i < new_size; i++) {
    BPlusTreePage *page = FetchBPlusTreePage(array_[i].second, buffer_pool_manager);
    page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(array_[i].second, true);
  }
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
  IncreaseSize(-1);
  int size = GetSize();
  for (int i = index; i < size; i++) {
    array_[i] = array_[i + 1];
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  return array_[0].second;
}
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int size_r = recipient->GetSize();
  recipient->CopyNFrom(&array_[0], size, buffer_pool_manager);
  recipient->SetKeyAt(size_r, middle_key);
  SetSize(0);
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  recipient->CopyLastFrom(std::make_pair(middle_key, array_[0].second), buffer_pool_manager);
  IncreaseSize(-1);
  int size = GetSize();
  for (int i = 0; i < size; i++) {
    array_[i] = array_[i + 1];
  }
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  array_[size] = pair;
  BPlusTreePage *page = FetchBPlusTreePage(pair.second, buffer_pool_manager);
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  IncreaseSize(-1);
  int size = GetSize();
  recipient->CopyFirstFrom(std::make_pair(middle_key, array_[size].second), buffer_pool_manager);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  for (int i = size; i >= 1; i--) {
    array_[i] = array_[i - 1];
  }
  array_[1].first = pair.first;
  array_[0].second = pair.second;
  BPlusTreePage *page = FetchBPlusTreePage(pair.second, buffer_pool_manager);
  page->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
