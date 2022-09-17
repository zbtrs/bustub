//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);
  pinned_num_ = 0;

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
  free_list_.clear();
}

bool BufferPoolManagerInstance::findFrame(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (replacer_ ->Victim(frame_id)) {
    //如果要替换的帧在page_table_中，就要改写，这里可以另外用一个map
    if (reverse_page_table_.find(*frame_id) != reverse_page_table_.end()) {
      Page* replaced_page = &pages_[*frame_id];
      if (replaced_page -> is_dirty_) {
        disk_manager_ ->WritePage(replaced_page -> page_id_,replaced_page->data_);
        replaced_page -> pin_count_ = 0;
      }
      page_table_.erase(replaced_page -> page_id_);
      reverse_page_table_.erase(*frame_id);
    }
    return true;
  }

  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  disk_manager_ ->WritePage(page_id,pages_[page_table_[page_id]].data_);
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  //latch_.lock();
  for (auto it : page_table_) {
    disk_manager_ ->WritePage(it.first,pages_[it.second].data_);
  }
  //latch_.unlock();
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  bool flag = true;
  for (int i = 0; i < static_cast<int>(pool_size_); ++i) {
    if (pages_[i].pin_count_ == 0) {
      flag = false;
      break;
    }
  }
  if (flag) {
    latch_.unlock();
    return nullptr;
  }
  frame_id_t stored_frame;
  if (!findFrame(&stored_frame)) {
    latch_.unlock();
    return nullptr;
  }
  page_id_t new_page_id = AllocatePage();
  Page *new_page = &pages_[stored_frame];
  new_page -> page_id_ = new_page_id;
  new_page -> pin_count_++;
  replacer_ -> Pin(stored_frame);
  page_table_[new_page_id] = stored_frame;
  reverse_page_table_[stored_frame] = new_page_id;
  new_page -> is_dirty_ = false;
  disk_manager_ ->WritePage(new_page -> GetPageId(),new_page -> GetData());
  *page_id = new_page_id;
  latch_.unlock();

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page* page = &pages_[frame_id];
    page->pin_count_++;
    replacer_ ->Pin(frame_id);
    latch_.unlock();
    return page;
  }
  frame_id_t stored_frame;
  if (!findFrame(&stored_frame)) {
    latch_.unlock();
    return nullptr;
  }
  Page *new_page = &pages_[stored_frame];
  if (new_page -> is_dirty_) {
    disk_manager_ ->WritePage(new_page -> page_id_,new_page -> data_);
  }
  page_table_.erase(new_page -> page_id_);
  new_page -> page_id_ = page_id;
  new_page -> pin_count_++;
  replacer_ -> Pin(stored_frame);
  new_page -> is_dirty_ = false;
  replacer_ ->Pin(stored_frame);
  disk_manager_->ReadPage(page_id,new_page->GetData());
  page_table_[page_id] = stored_frame;
  reverse_page_table_[stored_frame] = page_id;
  latch_.unlock();

  return new_page;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.lock();
  //DeallocatePage(page_id);
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t stored_frame = page_table_[page_id];
  Page *delete_page = &pages_[stored_frame];
  if (delete_page -> pin_count_ > 0) {
    latch_.unlock();
    return false;
  }
  if (delete_page -> is_dirty_) {
    FlushPgImp(page_id);
  }
  DeallocatePage(page_id);
  reverse_page_table_.erase(stored_frame);
  page_table_.erase(page_id);
  free_list_.push_back(stored_frame);
  delete_page -> is_dirty_ = false;
  delete_page -> pin_count_ = 0;
  delete_page -> page_id_ = INVALID_PAGE_ID;

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t stored_frame = page_table_[page_id];
  Page *unpin_page = &pages_[stored_frame];
  //unpin_page -> is_dirty_ = is_dirty;
  if (is_dirty) { //notice
    unpin_page -> is_dirty_ = true;
  }
  if (unpin_page -> pin_count_ == 0) {
    latch_.unlock();
    return false;
  }
  unpin_page -> pin_count_--;
  if (unpin_page -> GetPinCount() == 0) {
    replacer_ ->Unpin(stored_frame);
  }

  latch_.unlock();
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
