//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { capcity_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (!replace_lst_.empty()) {
    *frame_id = replace_lst_.back();
    frame_map_.erase(replace_lst_.back());
    replace_lst_.pop_back();
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (frame_map_.count(frame_id) != 0U) {
    replace_lst_.erase(frame_map_[frame_id]);
    frame_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (frame_map_.count(frame_id) != 0U) {
    return;
  }
  if (replace_lst_.size() == capcity_) {
    frame_map_.erase(replace_lst_.back());
    replace_lst_.pop_back();
  }
  replace_lst_.push_front(frame_id);
  frame_map_[frame_id] = replace_lst_.begin();
}

size_t LRUReplacer::Size() {
  auto size = replace_lst_.size();
  return size;
}

}  // namespace bustub
