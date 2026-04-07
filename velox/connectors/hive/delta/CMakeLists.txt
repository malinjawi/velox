# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

velox_add_library(
  velox_hive_delta
  DeltaDeletionVectorReader.cpp
  DeltaRowIndexFinder.cpp
  DeltaSplit.cpp
  DeltaSplitReader.cpp
  DeltaUuidUtils.cpp
)

velox_link_libraries(
  velox_hive_delta
  velox_common_base
  velox_common_io
  velox_connector
  velox_file
  Folly::folly
)

velox_include_directories(
  velox_hive_delta
  PRIVATE ${CMAKE_BINARY_DIR}/_deps/roaring-src/cpp
          ${CMAKE_BINARY_DIR}/_deps/roaring-src/include)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()

if(${VELOX_ENABLE_BENCHMARKS})
  add_subdirectory(benchmarks)
endif()
