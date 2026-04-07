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

#pragma once

#include "velox/connectors/hive/delta/DeletionVector.h"
#include "velox/connectors/hive/delta/DeletionVectorReader.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"

#include <memory>
#include <string>
#include <unordered_map>

namespace facebook::velox::connector::hive::delta {

/**
 * DeletionVectorFilter provides efficient filtering of deleted rows during
 * table scans. It maintains a cache of loaded deletion vectors and provides
 * fast row-level filtering based on row indices.
 *
 * This is the critical read-path component that was missing from the initial
 * implementation. According to Delta Lake's design:
 * "When reading a Delta table at a version that contains DVs, care must be
 *  taken to 'ignore' (filter out) the invalid rows during scans."
 *
 * Usage:
 *   auto filter = DeletionVectorFilter::create(pool, fs);
 *   filter->loadDeletionVector(filePath, dvPath, offset, size);
 *   if (filter->isRowDeleted(filePath, rowIndex)) {
 *     // Skip this row
 *   }
 */
class DeletionVectorFilter {
 public:
  /**
   * Configuration for the deletion vector filter.
   */
  struct Config {
    /// Maximum number of deletion vectors to cache in memory
    size_t maxCacheSize = 1000;

    /// Whether to enable statistics tracking
    bool enableStats = true;

    /// Whether to preload DVs for all files in a split
    bool preloadDVs = true;
  };

  /**
   * Statistics for monitoring filter performance.
   */
  struct Stats {
    /// Number of DVs currently cached
    size_t cachedDVs = 0;

    /// Total number of DV loads
    uint64_t totalLoads = 0;

    /// Number of cache hits
    uint64_t cacheHits = 0;

    /// Number of cache misses
    uint64_t cacheMisses = 0;

    /// Total rows checked
    uint64_t rowsChecked = 0;

    /// Total rows filtered (deleted)
    uint64_t rowsFiltered = 0;

    /// Total time spent loading DVs (microseconds)
    uint64_t loadTimeMicros = 0;

    /// Total time spent checking rows (microseconds)
    uint64_t checkTimeMicros = 0;

    /// Cache hit rate (0.0 to 1.0)
    double cacheHitRate() const {
      if (totalLoads == 0) return 0.0;
      return static_cast<double>(cacheHits) / totalLoads;
    }

    /// Filter rate (0.0 to 1.0)
    double filterRate() const {
      if (rowsChecked == 0) return 0.0;
      return static_cast<double>(rowsFiltered) / rowsChecked;
    }
  };

  /**
   * Create a new deletion vector filter.
   *
   * @param pool Memory pool for allocations
   * @param fs File system for reading DV files
   * @param config Optional configuration
   * @return Shared pointer to the filter
   */
  static std::shared_ptr<DeletionVectorFilter> create(
      memory::MemoryPool* pool,
      filesystems::FileSystem* fs,
      const Config& config = Config());

  virtual ~DeletionVectorFilter() = default;

  /**
   * Load a deletion vector for a specific file.
   *
   * This method loads the DV from storage and caches it for future lookups.
   * If the DV is already cached, this is a no-op.
   *
   * @param filePath Path to the data file
   * @param dvPath Path to the deletion vector file
   * @param offset Offset within the DV file (for packed DVs)
   * @param sizeInBytes Size of the DV data
   * @throws VeloxException if loading fails
   */
  virtual void loadDeletionVector(
      const std::string& filePath,
      const std::string& dvPath,
      int32_t offset,
      int32_t sizeInBytes) = 0;

  /**
   * Load a deletion vector from inline data.
   *
   * @param filePath Path to the data file
   * @param inlineData Base85-encoded inline DV data
   * @throws VeloxException if loading fails
   */
  virtual void loadInlineDeletionVector(
      const std::string& filePath,
      const std::string& inlineData) = 0;

  /**
   * Check if a specific row is deleted.
   *
   * This is the hot path - must be extremely fast.
   *
   * @param filePath Path to the data file
   * @param rowIndex 0-based row index within the file
   * @return true if the row is deleted, false otherwise
   */
  virtual bool isRowDeleted(
      const std::string& filePath,
      int64_t rowIndex) const = 0;

  /**
   * Check if a file has an associated deletion vector.
   *
   * @param filePath Path to the data file
   * @return true if the file has a DV loaded
   */
  virtual bool hasDeletionVector(const std::string& filePath) const = 0;

  /**
   * Get the number of deleted rows for a file.
   *
   * @param filePath Path to the data file
   * @return Number of deleted rows, or 0 if no DV
   */
  virtual int64_t getDeletedRowCount(const std::string& filePath) const = 0;

  /**
   * Clear the DV cache for a specific file.
   *
   * @param filePath Path to the data file
   */
  virtual void clearDeletionVector(const std::string& filePath) = 0;

  /**
   * Clear all cached deletion vectors.
   */
  virtual void clearAll() = 0;

  /**
   * Get current statistics.
   *
   * @return Statistics object
   */
  virtual Stats getStats() const = 0;

  /**
   * Reset statistics counters.
   */
  virtual void resetStats() = 0;

 protected:
  DeletionVectorFilter() = default;
};

/**
 * Default implementation of DeletionVectorFilter.
 */
class DeletionVectorFilterImpl : public DeletionVectorFilter {
 public:
  DeletionVectorFilterImpl(
      memory::MemoryPool* pool,
      filesystems::FileSystem* fs,
      const Config& config);

  void loadDeletionVector(
      const std::string& filePath,
      const std::string& dvPath,
      int32_t offset,
      int32_t sizeInBytes) override;

  void loadInlineDeletionVector(
      const std::string& filePath,
      const std::string& inlineData) override;

  bool isRowDeleted(const std::string& filePath, int64_t rowIndex)
      const override;

  bool hasDeletionVector(const std::string& filePath) const override;

  int64_t getDeletedRowCount(const std::string& filePath) const override;

  void clearDeletionVector(const std::string& filePath) override;

  void clearAll() override;

  Stats getStats() const override;

  void resetStats() override;

 private:
  /// Load DV from storage (internal helper)
  std::shared_ptr<DeletionVector> loadDVFromStorage(
      const std::string& dvPath,
      int32_t offset,
      int32_t sizeInBytes);

  /// Evict oldest DV if cache is full
  void evictIfNeeded();

  /// Memory pool for allocations
  memory::MemoryPool* pool_;

  /// File system for reading DVs
  filesystems::FileSystem* fs_;

  /// Configuration
  Config config_;

  /// DV reader for loading from storage
  std::unique_ptr<DeletionVectorReader> reader_;

  /// Cache: filePath -> DeletionVector
  mutable std::unordered_map<std::string, std::shared_ptr<DeletionVector>>
      cache_;

  /// LRU tracking: filePath -> access order
  mutable std::unordered_map<std::string, uint64_t> accessOrder_;

  /// Current access counter for LRU
  mutable uint64_t accessCounter_ = 0;

  /// Statistics
  mutable Stats stats_;

  /// Mutex for thread safety
  mutable std::mutex mutex_;
};

} // namespace facebook::velox::connector::hive::delta

// Made with Bob
