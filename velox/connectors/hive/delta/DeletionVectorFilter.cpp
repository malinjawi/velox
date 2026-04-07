/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/connectors/hive/delta/DeletionVectorFilter.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/time/Timer.h"

#include <algorithm>

namespace facebook::velox::connector::hive::delta {

// Static factory method
std::shared_ptr<DeletionVectorFilter> DeletionVectorFilter::create(
    memory::MemoryPool* pool,
    filesystems::FileSystem* fs,
    const Config& config) {
  return std::make_shared<DeletionVectorFilterImpl>(pool, fs, config);
}

// Implementation constructor
DeletionVectorFilterImpl::DeletionVectorFilterImpl(
    memory::MemoryPool* pool,
    filesystems::FileSystem* fs,
    const Config& config)
    : pool_(pool), fs_(fs), config_(config) {
  VELOX_CHECK_NOT_NULL(pool, "Memory pool cannot be null");
  VELOX_CHECK_NOT_NULL(fs, "File system cannot be null");
  
  // Create DV reader
  reader_ = std::make_unique<DeletionVectorReader>(pool, fs);
}

void DeletionVectorFilterImpl::loadDeletionVector(
    const std::string& filePath,
    const std::string& dvPath,
    int32_t offset,
    int32_t sizeInBytes) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Check if already cached
  if (cache_.find(filePath) != cache_.end()) {
    if (config_.enableStats) {
      stats_.cacheHits++;
      stats_.totalLoads++;
    }
    return;
  }

  // Track load time
  auto startTime = std::chrono::steady_clock::now();

  try {
    // Load DV from storage
    auto dv = loadDVFromStorage(dvPath, offset, sizeInBytes);
    
    // Evict if cache is full
    evictIfNeeded();
    
    // Add to cache
    cache_[filePath] = dv;
    accessOrder_[filePath] = ++accessCounter_;
    
    // Update stats
    if (config_.enableStats) {
      stats_.cacheMisses++;
      stats_.totalLoads++;
      stats_.cachedDVs = cache_.size();
      
      auto endTime = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
          endTime - startTime);
      stats_.loadTimeMicros += duration.count();
    }
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to load deletion vector for file '{}' from '{}' at offset {} "
        "with size {}: {}",
        filePath,
        dvPath,
        offset,
        sizeInBytes,
        e.what());
  }
}

void DeletionVectorFilterImpl::loadInlineDeletionVector(
    const std::string& filePath,
    const std::string& inlineData) {
  std::lock_guard<std::mutex> lock(mutex_);

  // Check if already cached
  if (cache_.find(filePath) != cache_.end()) {
    if (config_.enableStats) {
      stats_.cacheHits++;
      stats_.totalLoads++;
    }
    return;
  }

  // Track load time
  auto startTime = std::chrono::steady_clock::now();

  try {
    // Decode base85 inline data
    auto dv = reader_->readInline(inlineData);
    
    // Evict if cache is full
    evictIfNeeded();
    
    // Add to cache
    cache_[filePath] = dv;
    accessOrder_[filePath] = ++accessCounter_;
    
    // Update stats
    if (config_.enableStats) {
      stats_.cacheMisses++;
      stats_.totalLoads++;
      stats_.cachedDVs = cache_.size();
      
      auto endTime = std::chrono::steady_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
          endTime - startTime);
      stats_.loadTimeMicros += duration.count();
    }
  } catch (const std::exception& e) {
    VELOX_FAIL(
        "Failed to load inline deletion vector for file '{}': {}",
        filePath,
        e.what());
  }
}

bool DeletionVectorFilterImpl::isRowDeleted(
    const std::string& filePath,
    int64_t rowIndex) const {
  std::lock_guard<std::mutex> lock(mutex_);

  // Track check time
  auto startTime = std::chrono::steady_clock::now();

  // Find DV in cache
  auto it = cache_.find(filePath);
  if (it == cache_.end()) {
    // No DV for this file - row is not deleted
    return false;
  }

  // Update access order for LRU
  accessOrder_[filePath] = ++accessCounter_;

  // Check if row is deleted
  bool isDeleted = it->second->contains(rowIndex);

  // Update stats
  if (config_.enableStats) {
    stats_.rowsChecked++;
    if (isDeleted) {
      stats_.rowsFiltered++;
    }
    
    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
        endTime - startTime);
    stats_.checkTimeMicros += duration.count();
  }

  return isDeleted;
}

bool DeletionVectorFilterImpl::hasDeletionVector(
    const std::string& filePath) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return cache_.find(filePath) != cache_.end();
}

int64_t DeletionVectorFilterImpl::getDeletedRowCount(
    const std::string& filePath) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_.find(filePath);
  if (it == cache_.end()) {
    return 0;
  }
  
  return it->second->cardinality();
}

void DeletionVectorFilterImpl::clearDeletionVector(
    const std::string& filePath) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  cache_.erase(filePath);
  accessOrder_.erase(filePath);
  
  if (config_.enableStats) {
    stats_.cachedDVs = cache_.size();
  }
}

void DeletionVectorFilterImpl::clearAll() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  cache_.clear();
  accessOrder_.clear();
  
  if (config_.enableStats) {
    stats_.cachedDVs = 0;
  }
}

DeletionVectorFilter::Stats DeletionVectorFilterImpl::getStats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

void DeletionVectorFilterImpl::resetStats() {
  std::lock_guard<std::mutex> lock(mutex_);
  stats_ = Stats();
  stats_.cachedDVs = cache_.size();
}

// Private helper methods

std::shared_ptr<DeletionVector> DeletionVectorFilterImpl::loadDVFromStorage(
    const std::string& dvPath,
    int32_t offset,
    int32_t sizeInBytes) {
  // Use the reader to load from storage
  return reader_->read(dvPath, offset, sizeInBytes);
}

void DeletionVectorFilterImpl::evictIfNeeded() {
  // Check if cache is full
  if (cache_.size() < config_.maxCacheSize) {
    return;
  }

  // Find LRU entry (smallest access order)
  std::string lruFile;
  uint64_t minAccessOrder = UINT64_MAX;
  
  for (const auto& entry : accessOrder_) {
    if (entry.second < minAccessOrder) {
      minAccessOrder = entry.second;
      lruFile = entry.first;
    }
  }

  // Evict LRU entry
  if (!lruFile.empty()) {
    cache_.erase(lruFile);
    accessOrder_.erase(lruFile);
    
    if (config_.enableStats) {
      stats_.cachedDVs = cache_.size();
    }
  }
}

} // namespace facebook::velox::connector::hive::delta

// Made with Bob
