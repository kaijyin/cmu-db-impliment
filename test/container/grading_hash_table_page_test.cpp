//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_page_test.cpp
//
// Identification: test/container/hash_table_page_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/hash_table_block_page.h"
#include "storage/page/hash_table_header_page.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(HashTablePageTest, HeaderPageSampleTest) {
  DiskManager *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(3, disk_manager);

  // get a header page from the BufferPoolManager
  page_id_t header_page_id = INVALID_PAGE_ID;
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(bpm->NewPage(&header_page_id, nullptr)->GetData());

  // set some fields
  for (int i = 0; i < 11; i++) {
    header_page->SetSize(i);
    EXPECT_EQ(i, header_page->GetSize());
    header_page->SetPageId(i);
    EXPECT_EQ(i, header_page->GetPageId());
    header_page->SetLSN(i);
    EXPECT_EQ(i, header_page->GetLSN());
  }

  // add a few hypothetical block pages
  for (int i = 0; i < 10; i++) {
    header_page->AddBlockPageId(i);
    EXPECT_EQ(i + 1, header_page->NumBlocks());
  }

  // check for correct block page IDs
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(i, header_page->GetBlockPageId(i));
  }

  // unpin the header page now that we are done
  bpm->UnpinPage(header_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, BlockPageSampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(3, disk_manager);

  // get a block page from the BufferPoolManager
  page_id_t block_page_id = INVALID_PAGE_ID;

  auto block_page =
      reinterpret_cast<HashTableBlockPage<int, int, IntComparator> *>(bpm->NewPage(&block_page_id, nullptr)->GetData());

  // insert a few (key, value) pairs
  for (unsigned i = 0; i < 10; i++) {
    block_page->Insert(i, i, i);
  }

  // check for the inserted pairs
  for (unsigned i = 0; i < 10; i++) {
    EXPECT_EQ(i, block_page->KeyAt(i));
    EXPECT_EQ(i, block_page->ValueAt(i));
  }

  // remove a few pairs
  for (unsigned i = 0; i < 10; i++) {
    if (i % 2 == 1) {
      block_page->Remove(i);
    }
  }

  // check for the flags
  for (unsigned i = 0; i < 15; i++) {
    if (i < 10) {
      EXPECT_TRUE(block_page->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(block_page->IsReadable(i));
      } else {
        EXPECT_TRUE(block_page->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(block_page->IsOccupied(i));
    }
  }

  // unpin the header page now that we are done
  bpm->UnpinPage(block_page_id, true, nullptr);
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

// NOLINTNEXTLINE
TEST(HashTablePageTest, HashTablePageIntegratedTest) {
  size_t buffer_pool_size = 3;
  size_t hash_table_size = 500;
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  // setup: one header page and two block pages
  page_id_t header_page_id = INVALID_PAGE_ID;
  auto header_page = reinterpret_cast<HashTableHeaderPage *>(bpm->NewPage(&header_page_id, nullptr)->GetData());

  page_id_t block_page_id_1 = INVALID_PAGE_ID;
  page_id_t block_page_id_2 = INVALID_PAGE_ID;

  auto block_page_1 = reinterpret_cast<HashTableBlockPage<int, int, IntComparator> *>(
      bpm->NewPage(&block_page_id_1, nullptr)->GetData());
  auto block_page_2 = reinterpret_cast<HashTableBlockPage<int, int, IntComparator> *>(
      bpm->NewPage(&block_page_id_2, nullptr)->GetData());

  header_page->SetSize(hash_table_size);

  header_page->AddBlockPageId(block_page_id_1);
  header_page->AddBlockPageId(block_page_id_2);
  EXPECT_EQ(buffer_pool_size - 1, header_page->NumBlocks());

  // add to first block page until it is full
  size_t pairs_total_page_1 = hash_table_size / 2;
  for (unsigned i = 0; i < pairs_total_page_1; i++) {
    EXPECT_TRUE(block_page_1->Insert(i, i, i));
  }

  // add to second block page until it is full
  size_t pairs_total_page_2 = hash_table_size / 2;
  for (unsigned i = 0; i < pairs_total_page_2; i++) {
    EXPECT_TRUE(block_page_2->Insert(i, i, i));
  }

  // remove every other pair
  for (unsigned i = 0; i < pairs_total_page_1; i++) {
    if (i % 2 == 1) {
      block_page_1->Remove(i);
    }
  }

  for (unsigned i = 0; i < pairs_total_page_2; i++) {
    if (i % 2 == 1) {
      block_page_2->Remove(i);
    }
  }

  // check for the flags
  for (unsigned i = 0; i < pairs_total_page_1 + pairs_total_page_1 / 2; i++) {
    if (i < pairs_total_page_1) {
      EXPECT_TRUE(block_page_1->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(block_page_1->IsReadable(i));
      } else {
        EXPECT_TRUE(block_page_1->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(block_page_1->IsOccupied(i));
    }
  }

  for (unsigned i = 0; i < pairs_total_page_2 + pairs_total_page_2 / 2; i++) {
    if (i < pairs_total_page_2) {
      EXPECT_TRUE(block_page_2->IsOccupied(i));
      if (i % 2 == 1) {
        EXPECT_FALSE(block_page_2->IsReadable(i));
      } else {
        EXPECT_TRUE(block_page_2->IsReadable(i));
      }
    } else {
      EXPECT_FALSE(block_page_2->IsOccupied(i));
    }
  }

  bpm->UnpinPage(block_page_id_1, true, nullptr);
  bpm->UnpinPage(block_page_id_2, true, nullptr);
  bpm->UnpinPage(header_page_id, true, nullptr);

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub