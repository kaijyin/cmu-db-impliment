//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_tmp_tuple_page_test.cpp
//
// Identification: test/storage/grading_tmp_tuple_page_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "gtest/gtest.h"
#include "storage/page/tmp_tuple_page.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(GradingTmpTuplePageTest, DISABLED_BasicTest) {
  // Single insert of an (int, int).
  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  // Insert (123,456)
  {
    std::vector<Column> columns;
    columns.emplace_back("A", TypeId::INTEGER);
    columns.emplace_back("B", TypeId::INTEGER);
    Schema schema(columns);

    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetIntegerValue(123));
    values.emplace_back(ValueFactory::GetIntegerValue(456));

    Tuple tuple(values, &schema);
    ASSERT_EQ(tuple.GetLength(), 8);
    TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
    page.Insert(tuple, &tmp_tuple);

    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 12);
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 12), 8);
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 8), 123);
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + PAGE_SIZE - 4), 456);
  }
}

// NOLINTNEXTLINE
TEST(GradingTmpTuplePageTest, AdvancedTest) {
  // Multiple inserts of (int, int).
  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::INTEGER);
  Schema schema(columns);

  for (int i = 0; i < 300; i++) {
    std::vector<Value> values;
    values.emplace_back(ValueFactory::GetIntegerValue(i * 15));
    values.emplace_back(ValueFactory::GetIntegerValue(i * 455));

    Tuple tuple(values, &schema);
    ASSERT_EQ(tuple.GetLength(), 8);
    TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
    page.Insert(tuple, &tmp_tuple);

    uint32_t expected_offset = 4096 - 12 * (i + 1);

    ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
    ASSERT_EQ(tmp_tuple.GetOffset(), expected_offset);

    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 12 * (i + 1));
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + expected_offset), 8);
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + expected_offset + 4), i * 15);
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + expected_offset + 8), i * 455);
  }
}

// NOLINTNEXTLINE
TEST(GradingTmpTuplePageTest, EvilTest) {
  // Multiple inserts of (int, smallint, tinyint, bigint).
  TmpTuplePage page{};
  page_id_t page_id = 15445;
  page.Init(page_id, PAGE_SIZE);

  char *data = page.GetData();
  ASSERT_EQ(*reinterpret_cast<page_id_t *>(data), page_id);
  ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE);

  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::SMALLINT);
  columns.emplace_back("B", TypeId::TINYINT);
  columns.emplace_back("B", TypeId::BIGINT);
  Schema schema(columns);

  for (int i = 0; i < 200; i++) {
    std::vector<Value> values;
    uint32_t i1 = i * 420;
    uint16_t i2 = i * 69;
    uint8_t i3 = i * 42;
    uint64_t i4 = i * 5032;
    values.emplace_back(ValueFactory::GetIntegerValue(i1));
    values.emplace_back(ValueFactory::GetSmallIntValue(i2));
    values.emplace_back(ValueFactory::GetTinyIntValue(i3));
    values.emplace_back(ValueFactory::GetBigIntValue(i4));

    Tuple tuple(values, &schema);
    ASSERT_EQ(tuple.GetLength(), 15);
    TmpTuple tmp_tuple(INVALID_PAGE_ID, 0);
    page.Insert(tuple, &tmp_tuple);

    uint32_t expected_offset = 4096 - 19 * (i + 1);

    ASSERT_EQ(tmp_tuple.GetPageId(), page_id);
    ASSERT_EQ(tmp_tuple.GetOffset(), expected_offset);

    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + sizeof(page_id_t) + sizeof(lsn_t)), PAGE_SIZE - 19 * (i + 1));
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + expected_offset), 15);
    ASSERT_EQ(*reinterpret_cast<uint32_t *>(data + expected_offset + 4), i1);
    ASSERT_EQ(*reinterpret_cast<uint16_t *>(data + expected_offset + 8), i2);
    ASSERT_EQ(*reinterpret_cast<uint8_t *>(data + expected_offset + 10), i3);
    ASSERT_EQ(*reinterpret_cast<uint64_t *>(data + expected_offset + 11), i4);
  }
}

}  // namespace bustub
