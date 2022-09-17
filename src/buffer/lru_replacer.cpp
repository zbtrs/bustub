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
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  size_ = 0;
  capacity_ = num_pages;
  head_ = new ListNode();
  tail_ = new ListNode();
  head_->next_ = tail_;
  tail_->prev_ = head_;
}

LRUReplacer::~LRUReplacer() {
  // 释放整个链表的内存
  ListNode *temp;
  for (auto it = head_->next_; it != tail_; it = temp) {
    temp = it->next_;
    delete it;
  }
  delete head_;
  delete tail_;
}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool {
  mtx_.lock();
  if (size_ == 0) {
    mtx_.unlock();
    return false;
  }
  ListNode *tail_node = head_->next_;
  *frame_id = tail_node->frame_id_;
  tail_node->Remove();
  delete tail_node;
  --size_;
  hash_map_.erase(*frame_id);
  mtx_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mtx_.lock();
  if (hash_map_.find(frame_id) != hash_map_.end()) {
    ListNode *object_node = hash_map_[frame_id];
    object_node->Remove();
    hash_map_.erase(frame_id);
    delete object_node;
    --size_;
  }
  mtx_.unlock();
}

void LRUReplacer::AddTail(ListNode *new_node) {
  (tail_->prev_)->next_ = new_node;
  new_node->prev_ = tail_->prev_;
  new_node->next_ = tail_;
  tail_->prev_ = new_node;
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mtx_.lock();
  if (hash_map_.find(frame_id) == hash_map_.end()) {
    while (size_ >= capacity_) {
      ListNode *object_node = head_->next_;
      object_node->Remove();
      hash_map_.erase(object_node->frame_id_);
      delete object_node;
      --size_;
    }
    auto *new_node = new ListNode(frame_id);
    AddTail(new_node);
    hash_map_[frame_id] = new_node;
    ++size_;
  }
  mtx_.unlock();
}

auto LRUReplacer::Size() -> size_t { return size_; }

}  // namespace bustub
