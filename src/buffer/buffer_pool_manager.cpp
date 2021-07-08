//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
//  replacer_ = new LRUReplacer(pool_size);
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
  // TODO: nullify all Page Objects
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  // search for requested p in page table
  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = page_table_.find(page_id);

  // if found the page, pin it and return it immediately
  frame_id_t frameId = -1;
  if (iter != page_table_.end()) {
    frameId = iter->second;
    pages_[frameId].pin_count_++;
    replacer_->Pin(page_id);
    latch_.unlock();
    return &pages_[frameId];
  }

  // first try to find the page from the free list
  if (!free_list_.empty()) {
    frameId = free_list_.front();
    free_list_.pop_front();
  } else {
    // try to victimize a page
    if (!replacer_->Victim(&frameId)) {
      latch_.unlock();
      return nullptr;
    }
  }
  // if the page is dirty, write back
  if (pages_[frameId].IsDirty()) {
    page_id_t pageIdToDelete = pages_[frameId].GetPageId();
    FlushPageImplWithoutLock(pageIdToDelete);
    page_table_.erase(pageIdToDelete);
  }
  // Update P's metadata, read in the page content from disk
  pages_[frameId].ResetMemory();
  pages_[frameId].page_id_ = page_id; // used to be pageIdToDelete
  pages_[frameId].is_dirty_ = false;
  pages_[frameId].pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frameId].GetData());

  page_table_.insert({page_id, frameId});

  latch_.unlock();
  return &pages_[frameId];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();

  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = page_table_.find(page_id);

  // If the page to unpin cannot be found
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }
  frame_id_t frameId = iter->second;
  if (pages_[frameId].GetPinCount() == 0) {
    latch_.unlock();
    return false;
  }
  pages_[frameId].pin_count_--;
  pages_[frameId].is_dirty_ = pages_[frameId].is_dirty_ || is_dirty;
  if (pages_[frameId].GetPinCount() == 0) {
    replacer_->Unpin(frameId);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImplWithoutLock(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // Check if page id is valid

  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = page_table_.find(page_id);

  // if cannot found the page, return false
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frameId = iter->second;
  // if the page is dirty, write back to disk
  if (pages_[frameId].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frameId].GetData());
    pages_[frameId].is_dirty_ = false;
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  // Check if page id is valid
  latch_.lock();
  bool res = FlushPageImplWithoutLock(page_id);
  latch_.unlock();
  return res;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();

  // ----------- cql's code starts here --------------------
  frame_id_t freeFrameId = INVALID_PAGE_ID;
  if (free_list_.size() > 0) {
    // pick a free page from the free list
    freeFrameId = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Victim(&freeFrameId)) {
      // can't find any page from neither free list nor replacer
      latch_.unlock();
      return nullptr;
    }
    // free list is empty, but found one victim from replacer
    page_id_t pageIdToDelete = pages_[freeFrameId].GetPageId();
    // is page from replacer is dirty, write back to disk before delete (flush)
    if (pages_[freeFrameId].is_dirty_) {
      FlushPageImplWithoutLock(pageIdToDelete);
    }
    page_table_.erase(pageIdToDelete);
  }

  // get the free frame id

  *page_id = disk_manager_->AllocatePage();
  pages_[freeFrameId].ResetMemory();
  pages_[freeFrameId].page_id_ = *page_id;
  pages_[freeFrameId].pin_count_ = 1;
  // todo: pages_[freeFrameId].is_dirty_ = true; //???
  page_table_.insert({*page_id, freeFrameId});

  latch_.unlock();
  return &pages_[freeFrameId];
}


bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();
  std::unordered_map<page_id_t, frame_id_t>::const_iterator iter = page_table_.find(page_id);

  // If the page to delete doesn't exist, return true
  if (iter == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frameId = iter->second;

  if (pages_[frameId].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }

  // P can be deleted
  // Remove P from the page table
  page_table_.erase(page_id);
  // Reset metadata
  pages_[frameId].ResetMemory();
  pages_[frameId].page_id_ = INVALID_PAGE_ID;
  pages_[frameId].pin_count_ = 0;
  pages_[frameId].is_dirty_ = false;
  // Return it to free_list
  free_list_.push_back(frameId);
  // Deallocate from disk manager
  disk_manager_->DeallocatePage(page_id);

  latch_.unlock();
  return true;
}


void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  for(int i=0; i<(int)pool_size_; i++) {
    page_id_t page_id = pages_[i].page_id_;
    // skip invalid page_id
    if (page_id == INVALID_PAGE_ID) {
      continue;
    }
    FlushPageImplWithoutLock(page_id);
  }
  latch_.unlock();
}


}  // namespace bustub
