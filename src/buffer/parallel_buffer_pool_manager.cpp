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
#include "common/logger.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstance
  num_instances_ = num_instances;
  pool_size_ = pool_size;
  disk_manager_ = disk_manager;
  log_manager_ = log_manager;
  buffer_pools_ = new BufferPoolManagerInstance*[num_instances];
  allocated_ = new bool[num_instances];
  for (int i = 0; i < static_cast<int>(num_instances); ++i) {
    allocated_[i] = false;
  }
  start_index_ = 0;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (int i = 0; i < static_cast<int>(num_instances_); ++i) {
    if (allocated_[i]) {
      delete buffer_pools_[i];
    }
  }
  delete[] buffer_pools_;
  delete[] allocated_;
}

auto ParallelBufferPoolManager::GetPoolSize() -> size_t {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

auto ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) -> BufferPoolManagerInstance * {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  auto index = buffer_pool_table_[page_id % num_instances_];
  if (!allocated_[index]) {
    allocated_[index] = true;
    buffer_pools_[index] = new BufferPoolManagerInstance(pool_size_,disk_manager_,log_manager_);
  }
  return buffer_pools_[index];
}

auto ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) -> Page * {
  latch_.lock();
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance* buffer_pool = GetBufferPoolManager(page_id);
  latch_.unlock();
  return buffer_pool ->FetchPgImp(page_id);
}

auto ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  latch_.lock();
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance* buffer_pool = GetBufferPoolManager(page_id);
  latch_.unlock();
  return buffer_pool ->UnpinPgImp(page_id,is_dirty);
}

auto ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) -> bool {
  // Flush page_id from responsible BufferPoolManagerInstance
  latch_.lock();
  BufferPoolManagerInstance* buffer_pool = GetBufferPoolManager(page_id);
  latch_.unlock();
  return buffer_pool ->FlushPgImp(page_id);
}

auto ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) -> Page * {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  latch_.lock();
  auto x = start_index_;
  Page* result = nullptr;
  do {
    BufferPoolManagerInstance*& buffer_pool = buffer_pools_[x];
    if (!allocated_[x]) {
      allocated_[x] = true;
      buffer_pool = new BufferPoolManagerInstance(pool_size_,disk_manager_,log_manager_);
    }
    result = buffer_pool ->NewPgImp(page_id);
    if (result == nullptr) {
      ++x;
      if (x == num_instances_) {
        x = 0;
      }
    }
    else {
      start_index_++;
      if (start_index_ == num_instances_) {
        start_index_ = 0;
      }
      latch_.unlock();
      return result;
    }
  }while (x != start_index_);
  start_index_++;
  if (start_index_ == num_instances_) {
    start_index_ = 0;
  }
  latch_.unlock();
  return result;
}

auto ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) -> bool {
  // Delete page_id from responsible BufferPoolManagerInstance
  latch_.lock();
  BufferPoolManagerInstance* buffer_pool = GetBufferPoolManager(page_id);
  latch_.unlock();
  return buffer_pool ->DeletePgImp(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  latch_.lock();
  for (int i = 0; i < static_cast<int>(num_instances_); ++i) {
    if (buffer_pools_[i] != nullptr) {
      buffer_pools_[i] -> FlushAllPgsImp();
    }
  }
  latch_.unlock();
}

}  // namespace bustub
