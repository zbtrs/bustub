//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <map>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

struct ListNode {
  frame_id_t frame_id_{-1};
  ListNode *prev_{nullptr};
  ListNode *next_{nullptr};
  explicit ListNode(frame_id_t frame_id) : frame_id_(frame_id) {}
  ListNode() = default;
  void Remove() {
    prev_->next_ = next_;
    next_->prev_ = prev_;
  }
};

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  void AddTail(ListNode *new_node);

  size_t size_;
  size_t capacity_;
  std::unordered_map<frame_id_t, ListNode *> hash_map_{};
  ListNode *head_;
  ListNode *tail_;
  std::mutex mtx_;
};

}  // namespace bustub
