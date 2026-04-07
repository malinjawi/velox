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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <glog/logging.h>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/delta/DeltaDeletionVectorReader.h"
#include "velox/connectors/hive/storage_adapters/hdfs/RegisterHdfsFileSystem.h"

using namespace facebook::velox;
using namespace facebook::velox::connector::hive;

namespace {

// Test data directory
const std::string kTestDataDir = "/tmp/dv_benchmark_data";

// Helper to create a Delta-framed DV file
void createDeltaDVFile(
    const std::string& path,
    const std::vector<uint64_t>& deletedRows) {
  filesystems::registerLocalFileSystem();
  
  auto fs = filesystems::getFileSystem(path, nullptr);
  auto writeFile = fs->openFileForWrite(path);
  
  // Create RoaringBitmap
  roaring::Roaring64Map bitmap;
  for (auto row : deletedRows) {
    bitmap.add(row);
  }
  
  // Serialize to buffer
  size_t bitmapSize = bitmap.getSizeInBytes();
  std::vector<char> bitmapData(bitmapSize);
  bitmap.write(bitmapData.data());
  
  // Write Delta framing: [4 bytes size BE] + [payload] + [4 bytes CRC-32 BE]
  uint32_t payloadSize = static_cast<uint32_t>(bitmapSize);
  uint32_t payloadSizeBE = folly::Endian::big(payloadSize);
  
  // Calculate CRC-32
  uint32_t crc = folly::crc32c(
      reinterpret_cast<const uint8_t*>(bitmapData.data()), bitmapSize);
  uint32_t crcBE = folly::Endian::big(crc);
  
  // Write framed data
  writeFile->append(std::string_view(
      reinterpret_cast<const char*>(&payloadSizeBE), sizeof(payloadSizeBE)));
  writeFile->append(std::string_view(bitmapData.data(), bitmapSize));
  writeFile->append(
      std::string_view(reinterpret_cast<const char*>(&crcBE), sizeof(crcBE)));
  
  writeFile->close();
}

// Benchmark fixtures
class DVBenchmarkFixture {
 public:
  DVBenchmarkFixture() {
    filesystems::registerLocalFileSystem();
    
    // Create test data directory
    auto fs = filesystems::getFileSystem(kTestDataDir, nullptr);
    try {
      fs->mkdir(kTestDataDir);
    } catch (...) {
      // Directory might already exist
    }
    
    // Create small DV (100 deleted rows)
    std::vector<uint64_t> smallDeleted;
    for (uint64_t i = 0; i < 100; i++) {
      smallDeleted.push_back(i * 10); // Sparse pattern
    }
    createDeltaDVFile(kTestDataDir + "/small_dv.bin", smallDeleted);
    
    // Create medium DV (10K deleted rows)
    std::vector<uint64_t> mediumDeleted;
    for (uint64_t i = 0; i < 10000; i++) {
      mediumDeleted.push_back(i);
    }
    createDeltaDVFile(kTestDataDir + "/medium_dv.bin", mediumDeleted);
    
    // Create large DV (1M deleted rows)
    std::vector<uint64_t> largeDeleted;
    for (uint64_t i = 0; i < 1000000; i++) {
      largeDeleted.push_back(i);
    }
    createDeltaDVFile(kTestDataDir + "/large_dv.bin", largeDeleted);
  }
  
  ~DVBenchmarkFixture() {
    // Cleanup test files
    auto fs = filesystems::getFileSystem(kTestDataDir, nullptr);
    try {
      fs->remove(kTestDataDir + "/small_dv.bin");
      fs->remove(kTestDataDir + "/medium_dv.bin");
      fs->remove(kTestDataDir + "/large_dv.bin");
      fs->rmdir(kTestDataDir);
    } catch (...) {
      // Ignore cleanup errors
    }
  }
};

// Global fixture
std::unique_ptr<DVBenchmarkFixture> g_fixture;

void ensureFixture() {
  if (!g_fixture) {
    g_fixture = std::make_unique<DVBenchmarkFixture>();
  }
}

} // namespace

// Benchmark: Small DV (100 rows)
BENCHMARK(SmallDV_100Rows) {
  ensureFixture();
  folly::BenchmarkSuspender suspender;
  
  DeltaDeletionVectorReader reader;
  std::string path = "file://" + kTestDataDir + "/small_dv.bin";
  
  suspender.dismiss();
  
  auto bitmap = reader.read(path, 0, 0);
  
  folly::doNotOptimizeAway(bitmap);
}

// Benchmark: Medium DV (10K rows)
BENCHMARK_RELATIVE(MediumDV_10KRows) {
  ensureFixture();
  folly::BenchmarkSuspender suspender;
  
  DeltaDeletionVectorReader reader;
  std::string path = "file://" + kTestDataDir + "/medium_dv.bin";
  
  suspender.dismiss();
  
  auto bitmap = reader.read(path, 0, 0);
  
  folly::doNotOptimizeAway(bitmap);
}

// Benchmark: Large DV (1M rows)
BENCHMARK_RELATIVE(LargeDV_1MRows) {
  ensureFixture();
  folly::BenchmarkSuspender suspender;
  
  DeltaDeletionVectorReader reader;
  std::string path = "file://" + kTestDataDir + "/large_dv.bin";
  
  suspender.dismiss();
  
  auto bitmap = reader.read(path, 0, 0);
  
  folly::doNotOptimizeAway(bitmap);
}

BENCHMARK_DRAW_LINE();

// Benchmark: Row lookup in small DV
BENCHMARK(RowLookup_SmallDV) {
  ensureFixture();
  folly::BenchmarkSuspender suspender;
  
  DeltaDeletionVectorReader reader;
  std::string path = "file://" + kTestDataDir + "/small_dv.bin";
  auto bitmap = reader.read(path, 0, 0);
  
  suspender.dismiss();
  
  // Check 1000 rows
  for (uint64_t i = 0; i < 1000; i++) {
    bool deleted = bitmap->contains(i);
    folly::doNotOptimizeAway(deleted);
  }
}

// Benchmark: Row lookup in large DV
BENCHMARK_RELATIVE(RowLookup_LargeDV) {
  ensureFixture();
  folly::BenchmarkSuspender suspender;
  
  DeltaDeletionVectorReader reader;
  std::string path = "file://" + kTestDataDir + "/large_dv.bin";
  auto bitmap = reader.read(path, 0, 0);
  
  suspender.dismiss();
  
  // Check 1000 rows
  for (uint64_t i = 0; i < 1000; i++) {
    bool deleted = bitmap->contains(i);
    folly::doNotOptimizeAway(deleted);
  }
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  
  LOG(INFO) << "Delta Deletion Vector Benchmark";
  LOG(INFO) << "================================";
  
  folly::runBenchmarks();
  
  return 0;
}

// Made with Bob
