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
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  mu_.lock();
  if (IsEmpty()) {
    return false;
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::READ);
  if (root_page_id_ != pre_root_id) {
    root->RUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    return GetValue(key, result, transaction);
  }

  Page *page = FindLeafPage(key, false, root, LockType::READ, transaction);
  UnlockAndUnpin(LockType::READ, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType value;
  bool res = false;
  if (leaf_node->Lookup(key, &value, comparator_)) {
    result->push_back(value);
    res = true;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return res;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  int statu = LuckyInsert(key, value, transaction);
  if (statu == 1) {
    return true;
  }
  if (statu == 0) {
    return false;
  }
  return SadInsert(key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::LuckyInsert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  mu_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    mu_.unlock();
    return 1;
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::READ);
  if (root_page_id_ != pre_root_id) {
    root->RUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    return -1;
  }

  Page *page = FindLeafPage(key, false, root, LockType::READ, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  if (leaf_node->KeyIndex(key, comparator_) != -1) {
    UnlockAndUnpin(LockType::READ, transaction);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return 0;
  }
  // if root,cant do lucky insert
  if (leaf_node->IsRootPage()) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return -1;
  }
  if (leaf_node->GetSize() + 1 < leaf_node->GetMaxSize()) {
    page->RUnlatch();
    page->WLatch();
    UnlockAndUnpin(LockType::READ, transaction);
    leaf_node->Insert(key, value, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return 1;
  }
  UnlockAndUnpin(LockType::READ, transaction);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return -1;
}
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::SadInsert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  mu_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    mu_.unlock();
    return true;
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::INSERT);
  if (root_page_id_ != pre_root_id) {
    root->WUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    return SadInsert(key, value, transaction);
  }
  return InsertIntoLeaf(key, value, root, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t new_root_id = INVALID_PAGE_ID;
  Page *page = NewPage(&new_root_id);
  LeafPage *root_node = reinterpret_cast<LeafPage *>(page->GetData());
  root_node->Init(new_root_id, INVALID_PAGE_ID, leaf_max_size_);
  root_node->Insert(key, value, comparator_);
  root_page_id_ = new_root_id;
  UpdateRootPageId(true);
  buffer_pool_manager_->UnpinPage(new_root_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Page *root, Transaction *transaction) {
  Page *page = FindLeafPage(key, false, root, LockType::INSERT, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  int pre_size = leaf_node->GetSize();
  int now_size = leaf_node->Insert(key, value, comparator_);

  if (pre_size == now_size) {
    UnlockAndUnpin(LockType::INSERT, transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
    return false;
  }

  if (leaf_node->GetSize() == leaf_node->GetMaxSize()) {
    LeafPage *new_node = Split(leaf_node);
    InsertIntoParent(leaf_node, new_node->KeyAt(0), new_node);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    page_id_t parent_page_id = leaf_node->GetParentPageId();
    Page *parent_page = FetchPage(parent_page_id);
    InternalPage *cur_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    while (cur_node->GetSize() == cur_node->GetMaxSize()) {
      InternalPage *new_inter_page = Split(cur_node);
      InsertIntoParent(cur_node, new_inter_page->KeyAt(0), new_inter_page);
      parent_page_id = cur_node->GetParentPageId();
      buffer_pool_manager_->UnpinPage(new_inter_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), true);
      parent_page = FetchPage(parent_page_id);
      cur_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    }
    // the lastone is not modified
    buffer_pool_manager_->UnpinPage(cur_node->GetPageId(), false);
  }
  UnlockAndUnpin(LockType::INSERT, transaction);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

// insert导致page容量超1,一分为二,处理父节点以及
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id = INVALID_PAGE_ID;
  Page *page = NewPage(&page_id);
  BPlusTreePage *pre_node = reinterpret_cast<BPlusTreePage *>(node);
  if (pre_node->IsLeafPage()) {
    LeafPage *leaf_node = reinterpret_cast<LeafPage *>(pre_node);
    LeafPage *sib_node = reinterpret_cast<LeafPage *>(page->GetData());
    sib_node->Init(page_id, leaf_node->GetParentPageId(), leaf_max_size_);
    leaf_node->MoveHalfTo(sib_node);
  } else {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(pre_node);
    InternalPage *sib_node = reinterpret_cast<InternalPage *>(page->GetData());
    sib_node->Init(page_id, internal_node->GetParentPageId(), internal_max_size_);
    internal_node->MoveHalfTo(sib_node, buffer_pool_manager_);
  }
  return reinterpret_cast<N *>(page->GetData());
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
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  page_id_t parent_page_id = old_node->GetParentPageId();
  if (parent_page_id == INVALID_PAGE_ID) {
    page_id_t new_root_id = INVALID_PAGE_ID;
    Page *page = NewPage(&new_root_id);
    InternalPage *new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    mu_.lock();
    root_page_id_ = new_root_id;
    UpdateRootPageId();
    mu_.unlock();
    return;
  }
  Page *page = FetchPage(parent_page_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
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
bool BPLUSTREE_TYPE::LuckyRemove(const KeyType &key, Transaction *transaction) {
  mu_.lock();
  if (IsEmpty()) {
    mu_.unlock();
    return true;
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::READ);
  if (root_page_id_ != pre_root_id) {
    root->RUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    return false;
  }
  Page *page = FindLeafPage(key, false, root, LockType::READ, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf_node->KeyIndex(key, comparator_) == -1) {
    UnlockAndUnpin(LockType::READ, transaction);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return true;
  }
  if (leaf_node->IsRootPage()) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  if (leaf_node->GetSize() > leaf_node->GetMinSize()) {
    page->RUnlatch();
    page->WLatch();
    UnlockAndUnpin(LockType::READ, transaction);
    leaf_node->RemoveAndDeleteRecord(key, comparator_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }
  UnlockAndUnpin(LockType::READ, transaction);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SadRemove(const KeyType &key, Transaction *transaction) {
  mu_.lock();
  if (IsEmpty()) {
    return;
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::DELETE);
  if (root_page_id_ != pre_root_id) {
    root->WUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    SadRemove(key, transaction);
    return;
  }
  // std::cout << "fetch root sadremove\n";
  Page *page = FindLeafPage(key, false, root, LockType::DELETE, transaction);
  // page statu:locked and pined,not in page_set
  // std::cout << "find leaf " << page->GetPageId() << "\n";
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int pre_size = leaf_page->GetSize();
  int now_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  if (pre_size == now_size) {
    UnlockAndUnpin(LockType::DELETE, transaction);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }

  page_id_t parent_page_id = leaf_page->GetParentPageId();
  bool del = false;
  if (leaf_page->IsRootPage()) {
    if (leaf_page->GetSize() == 0) {
      mu_.lock();
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
      mu_.unlock();
      del = true;
    }
    if (del) {
      transaction->AddIntoDeletedPageSet(page->GetPageId());
    }
  } else if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // std::cout << "need to col or merge\n";
    del = CoalesceOrRedistribute(leaf_page, transaction);
    if (del) {
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
    page_id_t root_id = root_page_id_;
    while (del && parent_page_id != root_id) {
      Page *parent_page = FetchPage(parent_page_id);
      InternalPage *cur_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
      parent_page_id = cur_node->GetParentPageId();
      del = false;
      if (cur_node->GetSize() < cur_node->GetMinSize()) {
        del = CoalesceOrRedistribute(cur_node, transaction);
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
        if (del) {
          transaction->AddIntoDeletedPageSet(parent_page->GetPageId());
        }
      } else {
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      }
    }
    if (del && parent_page_id == root_id) {
      Page *root_page = FetchPage(root_id);
      InternalPage *root = reinterpret_cast<InternalPage *>(root_page->GetData());
      if (root->GetSize() == 1) {
        mu_.lock();
        root_page_id_ = root->RemoveAndReturnOnlyChild();
        UpdateRootPageId();
        mu_.unlock();
        buffer_pool_manager_->UnpinPage(root_id, true);
        transaction->AddIntoDeletedPageSet(root_id);
      } else {
        buffer_pool_manager_->UnpinPage(root_id, false);
      }
    }
  }
  UnlockAndUnpin(LockType::DELETE, transaction);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::cout << "remove " << key << "\n";
  if (LuckyRemove(key, transaction)) {
    return;
  }
  // std::cout << "lucky remove failed,start sad remove\n";
  SadRemove(key, transaction);
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
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(node);
  page_id_t parent_id = cur_node->GetParentPageId();
  Page *parent_page = FetchPage(parent_id);
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  // if(parent->GetSize()==1){
  //   parent->Remove(0);
  //   buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
  //   return true;
  // }
  int index = parent->ValueIndex(cur_node->GetPageId());
  page_id_t merge_sib_id = INVALID_PAGE_ID;
  bool is_right = false;
  if (index > 0) {
    page_id_t sib_id = parent->ValueAt(index - 1);
    merge_sib_id = sib_id;
    Page *sib_page = FetchPage(sib_id, LockType::READ);
    BPlusTreePage *sib_node = reinterpret_cast<BPlusTreePage *>(sib_page->GetData());
    if (sib_node->GetSize() > sib_node->GetMinSize()) {
      sib_page->RUnlatch();
      sib_page->WLatch();
      Redistribute(sib_node, cur_node, false, parent, index);
      sib_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sib_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return false;
    }
    sib_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(sib_node->GetPageId(), false);
  }
  if (index + 1 < parent->GetSize()) {
    is_right = true;
    page_id_t sib_id = parent->ValueAt(index + 1);
    merge_sib_id = sib_id;
    Page *sib_page = FetchPage(sib_id, LockType::READ);
    BPlusTreePage *sib_node = reinterpret_cast<BPlusTreePage *>(sib_page->GetData());
    if (sib_node->GetSize() > sib_node->GetMinSize()) {
      sib_page->RUnlatch();
      sib_page->WLatch();
      Redistribute(sib_node, cur_node, true, parent, index + 1);
      sib_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(sib_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return false;
    }
    sib_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(sib_node->GetPageId(), false);
  }
  // 不能通过只移动一个的方式调整,那么就merge
  Page *sib_page = FetchPage(merge_sib_id, LockType::DELETE);
  BPlusTreePage *sib_node = reinterpret_cast<BPlusTreePage *>(sib_page->GetData());
  if (is_right) {
    Coalesce(cur_node, sib_node, parent, index + 1, transaction);
  } else {
    Coalesce(sib_node, cur_node, parent, index, transaction);
  }
  sib_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(merge_sib_id, true);
  buffer_pool_manager_->UnpinPage(parent_id, true);
  return true;
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
void BPLUSTREE_TYPE::Coalesce(N *neighbor_node, N *node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index,
                              Transaction *transaction) {
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(node);
  if (page->IsLeafPage()) {
    LeafPage *leaf_page_r = reinterpret_cast<LeafPage *>(page);
    LeafPage *leaf_page_l = reinterpret_cast<LeafPage *>(neighbor_node);
    leaf_page_r->MoveAllTo(leaf_page_l);
  } else {
    InternalPage *inter_page_r = reinterpret_cast<InternalPage *>(page);
    InternalPage *inter_page_l = reinterpret_cast<InternalPage *>(neighbor_node);
    inter_page_r->MoveAllTo(inter_page_l, parent->KeyAt(index), buffer_pool_manager_);
  }
  parent->Remove(index);
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, bool move_front,
                                  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent, int index) {
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(node);
  if (page->IsLeafPage()) {
    LeafPage *cur_page = reinterpret_cast<LeafPage *>(page);
    LeafPage *sib_page = reinterpret_cast<LeafPage *>(neighbor_node);
    if (move_front) {
      parent->SetKeyAt(index, sib_page->KeyAt(1));
      sib_page->MoveFirstToEndOf(cur_page);
    } else {
      parent->SetKeyAt(index, sib_page->KeyAt(sib_page->GetSize() - 1));
      sib_page->MoveLastToFrontOf(cur_page);
    }
  } else {
    InternalPage *cur_page = reinterpret_cast<InternalPage *>(page);
    InternalPage *sib_page = reinterpret_cast<InternalPage *>(neighbor_node);
    if (move_front) {
      sib_page->MoveFirstToEndOf(cur_page, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, sib_page->KeyAt(0));
    } else {
      sib_page->MoveLastToFrontOf(cur_page, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, sib_page->KeyAt(sib_page->GetSize()));
    }
  }
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
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType nullkey;
  mu_.lock();
  if (IsEmpty()) {
    return end();
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::READ);
  if (root_page_id_ != pre_root_id) {
    root->RUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    return begin();
  }
  Page *page = FindLeafPage(nullkey, true, root, LockType::READ);
  return INDEXITERATOR_TYPE(page, 0, buffer_pool_manager_, false);
}
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  mu_.lock();
  if (IsEmpty()) {
    return end();
  }
  page_id_t pre_root_id = root_page_id_;
  mu_.unlock();
  Page *root = FetchPage(pre_root_id, LockType::READ);
  if (root_page_id_ != pre_root_id) {
    root->RUnlatch();
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    return Begin(key);
  }
  Page *page = FindLeafPage(key, false, root, LockType::READ);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(page, leaf_node->KeyIndex(key, comparator_), buffer_pool_manager_, false);
}
/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, 0, nullptr, true); }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockAndUnpin(LockType lock_type, Transaction *transcation) {
  if (transcation == nullptr) {
    return;
  }
  std::deque<Page *> *page_set = transcation->GetPageSet().get();
  while (!page_set->empty()) {
    Page *page = page_set->front();
    if (lock_type == LockType::READ) {
      page->RUnlatch();
    } else {
      page->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page_set->pop_front();
  }
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N *node, LockType lock_type) {
  BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(node);
  if (lock_type == LockType::DELETE) {
    return cur_node->GetSize() > cur_node->GetMinSize();
  }
  return cur_node->GetSize() + 1 < cur_node->GetMaxSize();
}
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, Page *root, LockType lock_type,
                                   Transaction *transcation) {
  Page *page = root;
  InternalPage *inter_node = reinterpret_cast<InternalPage *>(page->GetData());
  while (!inter_node->IsLeafPage()) {
    if (lock_type == LockType::READ) {
      UnlockAndUnpin(lock_type, transcation);
    }
    if (transcation != nullptr) {
      transcation->AddIntoPageSet(page);
    }
    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = inter_node->ValueAt(0);
    } else {
      child_page_id = inter_node->Lookup(key, comparator_);
    }
    Page *child_page = FetchPage(child_page_id);
    inter_node = reinterpret_cast<InternalPage *>(child_page->GetData());
    if (lock_type == LockType::READ) {
      child_page->RLatch();
    } else {
      child_page->WLatch();
      if (IsSafe(inter_node, lock_type)) {
        UnlockAndUnpin(lock_type, transcation);
      }
    }
    if (transcation == nullptr) {
      if (lock_type == LockType::READ) {
        page->RUnlatch();
      } else {
        page->WUnlatch();
      }
    }
    page = child_page;
  }
  return page;
}
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FetchPage(page_id_t page_id, LockType lock_type) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "all page pined!");
  }
  if (lock_type == LockType::READ) {
    page->RLatch();
  } else if (lock_type == LockType::DELETE || lock_type == LockType::INSERT) {
    page->WLatch();
  }
  return page;
}
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::NewPage(page_id_t *page_id) {
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of memory!");
  }
  return page;
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
void BPLUSTREE_TYPE::UpdateRootPageId(bool insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record) {
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
 * This method is used for debug only, You don't  need to modify
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
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
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
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
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
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

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
