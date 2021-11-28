//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager, bool isEnd);
  ~IndexIterator();

  bool IsEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    if (itr.cur_page_id_ != cur_page_id_) {
      return false;
    }
    if (itr.now_index_ != now_index_) {
      return false;
    }
    return true;
  }

  bool operator!=(const IndexIterator &itr) const { return !operator==(itr); }

 private:
  // add your own private member variables here
  Page *page_;
  LeafPage *cur_node_;
  int cur_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  int now_index_;
};

}  // namespace bustub
