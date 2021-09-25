/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int index, BufferPoolManager *buffer_pool_manager, bool isEnd) {
  cur_node_ = reinterpret_cast<LeafPage *>(page->GetData());
  page_ = page;
  now_index_ = index;
  this->buffer_pool_manager_ = buffer_pool_manager;
  if (isEnd) {
    cur_page_id_ = INVALID_PAGE_ID;
  } else {
    cur_page_id_ = page->GetPageId();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (page_ != nullptr) {
    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return cur_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return cur_node_->GetItem(now_index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    return *this;
  }
  if (now_index_ == cur_node_->GetSize() - 1) {
    // std::cout<<"to the end and change page\n";
    int next_page_id = cur_node_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      page_->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
      cur_node_ = nullptr;
      page_ = nullptr;
      cur_page_id_ = INVALID_PAGE_ID;
    } else {
      Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
      if (next_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch error!");
      }
      next_page->RLatch();
      page_->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
      page_ = next_page;
      cur_node_ = reinterpret_cast<LeafPage *>(page_->GetData());
      cur_page_id_ = page_->GetPageId();
    }
    now_index_ = 0;
  } else {
    now_index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
