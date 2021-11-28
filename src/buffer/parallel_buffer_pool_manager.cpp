//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : num_instances_(num_instances) {
  buffer_pool_ = new BufferPoolManager *[num_instances_];
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    buffer_pool_[i] = new BufferPoolManagerInstance(pool_size, num_instances_, i, disk_manager, log_manager);
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
    delete buffer_pool_[i];
  }
  delete buffer_pool_;
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return buffer_pool_[0]->GetPoolSize() * num_instances_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  uint32_t idx = GetIdx(page_id);
  return buffer_pool_[idx];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  uint32_t idx = GetIdx(page_id);
  return buffer_pool_[idx]->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  uint32_t idx = GetIdx(page_id);
  return buffer_pool_[idx]->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  uint32_t idx = GetIdx(page_id);
  return buffer_pool_[idx]->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  Page *page = nullptr;
  uint32_t start_idx = next_idx_;
  while (page == nullptr) {
    page = buffer_pool_[next_idx_]->NewPage(page_id);
    next_idx_ = (next_idx_ + 1) % num_instances_;
    if (next_idx_ == start_idx) {
      break;
    }
  }
  return page;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  uint32_t idx = GetIdx(page_id);
  return buffer_pool_[idx]->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    buffer_pool_[i]->FlushAllPages();
  }
}

}  // namespace bustub
