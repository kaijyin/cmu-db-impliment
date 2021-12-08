//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// grading_recovery_Test.cpp
//
// Identification: test/recovery/grading_recovery_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <vector>

#include "common/bustub_instance.h"
#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"
#include "logging/common.h"
#include "recovery/log_recovery.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_RedoTestWithOneTxn) {
  remove("test.db");
  remove("test.log");

  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_4 = tuple1.GetValue(&schema, 4);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  ASSERT_TRUE(test_table->InsertTuple(tuple1, &rid1, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("System restart...");
  bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Check if tuple is not in table before recovery");
  Tuple old_tuple;
  Tuple old_tuple1;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Begin recovery");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  LOG_INFO("Redo underway...");
  log_recovery->Redo();
  LOG_INFO("Undo underway...");
  log_recovery->Undo();

  LOG_INFO("Check if recovery success");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 4).CompareEquals(val1_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 0).CompareEquals(val1_0), CmpBool::CmpTrue);

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_UndoTestWithOneTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  RID rid;
  const Tuple tuple = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_1 = tuple.GetValue(&schema, 1);

  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushPage(first_page_id);

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("System restarted..");
  bustub_instance = new BustubInstance("test.db");

  LOG_INFO("Check if tuple exists before recovery");
  Tuple old_tuple;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  LOG_INFO("Check if failed txn is undo successfully");
  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  bustub_instance->transaction_manager_->Commit(txn);

  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_BasicRedoTestWithOneTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  RID rid;
  RID rid2;
  RID rid3;
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_4 = tuple1.GetValue(&schema, 4);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  auto val3_0 = tuple3.GetValue(&schema, 0);
  auto val3_1 = tuple3.GetValue(&schema, 1);
  auto val3_2 = tuple3.GetValue(&schema, 2);
  auto val3_3 = tuple3.GetValue(&schema, 3);
  auto val3_4 = tuple3.GetValue(&schema, 4);

  ASSERT_EQ(bustub_instance->disk_manager_->GetNumFlushes(), 0);

  LOG_INFO("Insert tuple");
  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  LOG_INFO("Update tuple to tuple1");
  ASSERT_TRUE(test_table->UpdateTuple(tuple1, rid, txn));
  LOG_INFO("Insert tuple2");
  ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid2, txn));
  LOG_INFO("Delete tuple2");
  ASSERT_TRUE(test_table->MarkDelete(rid2, txn));
  LOG_INFO("Insert tuple3");
  ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid3, txn));

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  delete txn;
  delete test_table;

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("System restart..");
  bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Check if tuple is not in table before recovery");
  Tuple old_tuple;
  Tuple old_tuple1;
  Tuple old_tuple2;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple1, txn));
  ASSERT_FALSE(test_table->GetTuple(rid3, &old_tuple2, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_TRUE(test_table->GetTuple(rid3, &old_tuple2, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple1, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpFalse);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpFalse);
  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val1_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val1_0), CmpBool::CmpTrue);

  ASSERT_EQ(old_tuple2.GetValue(&schema, 0).CompareEquals(val3_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 1).CompareEquals(val3_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 2).CompareEquals(val3_2), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 3).CompareEquals(val3_3), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 4).CompareEquals(val3_4), CmpBool::CmpTrue);

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_BasicUndoTestWithOneTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  RID rid;
  RID rid2;
  RID rid3;
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_4 = tuple1.GetValue(&schema, 4);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  auto val3_0 = tuple3.GetValue(&schema, 0);
  auto val3_1 = tuple3.GetValue(&schema, 1);
  auto val3_2 = tuple3.GetValue(&schema, 2);
  auto val3_3 = tuple3.GetValue(&schema, 3);
  auto val3_4 = tuple3.GetValue(&schema, 4);

  LOG_INFO("Insert tuple");
  ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
  LOG_INFO("Update tuple to tuple1");
  ASSERT_TRUE(test_table->UpdateTuple(tuple1, rid, txn));
  LOG_INFO("Insert tuple2");
  ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid2, txn));
  LOG_INFO("Delete tuple2");
  ASSERT_TRUE(test_table->MarkDelete(rid2, txn));
  LOG_INFO("Insert tuple3");
  ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid3, txn));

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushPage(first_page_id);

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("System restarted..");
  bustub_instance = new BustubInstance("test.db");

  LOG_INFO("Check if tuple exists before recovery");
  ASSERT_FALSE(enable_logging);
  Tuple old_tuple;
  Tuple old_tuple1;
  Tuple old_tuple2;
  Tuple old_tuple3;
  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_TRUE(test_table->GetTuple(rid3, &old_tuple2, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple1, txn));
  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;

  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpFalse);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpFalse);
  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val1_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val1_0), CmpBool::CmpTrue);

  ASSERT_EQ(old_tuple2.GetValue(&schema, 0).CompareEquals(val3_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 1).CompareEquals(val3_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 2).CompareEquals(val3_2), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 3).CompareEquals(val3_3), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 4).CompareEquals(val3_4), CmpBool::CmpTrue);

  LOG_INFO("Recovery started..");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  txn = bustub_instance->transaction_manager_->Begin();
  delete test_table;
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_FALSE(test_table->GetTuple(rid3, &old_tuple3, txn));
  ASSERT_FALSE(test_table->GetTuple(rid2, &old_tuple2, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_RedoTestWithMultipleTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  bustub_instance->transaction_manager_->Commit(txn);

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  // Create 2 tuples
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);

  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_4 = tuple.GetValue(&schema, 4);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_2 = tuple1.GetValue(&schema, 2);

  const int num_threads = 2;
  std::vector<std::thread> threads;

  // thread 0
  threads.emplace_back(std::thread([&test_table, &tuple, &tuple2, &tuple3, bustub_instance, &rid]() {
    Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple2, rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple, rid, txn1));
    bustub_instance->transaction_manager_->Commit(txn1);
    delete txn1;
  }));

  // thread 1
  threads.emplace_back(std::thread([&test_table, &tuple1, &tuple2, &tuple3, bustub_instance, &rid1]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple2, rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple2, rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple1, rid1, txn2));
    bustub_instance->transaction_manager_->Commit(txn2);
    delete txn2;
  }));

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Recovery started..");
  bustub_instance = new BustubInstance("test.db");
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  Tuple old_tuple;
  Tuple old_tuple1;

  txn = bustub_instance->transaction_manager_->Begin();

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);

  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  ASSERT_EQ(old_tuple1.GetValue(&schema, 1).CompareEquals(val1_1), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 2).CompareEquals(val1_2), CmpBool::CmpTrue);

  bustub_instance->transaction_manager_->Commit(txn);

  delete txn;
  delete log_recovery;
  delete bustub_instance;
  delete test_table;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_UndoTestWithMultipleTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  bustub_instance->transaction_manager_->Commit(txn);

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  // Create 2 tuples
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_4 = tuple.GetValue(&schema, 4);
  auto val1_1 = tuple1.GetValue(&schema, 1);
  auto val1_2 = tuple1.GetValue(&schema, 2);

  const int num_threads = 2;
  std::vector<std::thread> threads;

  // thread 0
  threads.emplace_back(std::thread([&test_table, &tuple, &tuple2, &tuple3, bustub_instance, &rid]() {
    Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple2, rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple, rid, txn1));
    delete txn1;
  }));

  // thread 1
  threads.emplace_back(std::thread([&test_table, &tuple1, &tuple2, &tuple3, bustub_instance, &rid1]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple2, rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple2, rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple1, rid1, txn2));
    delete txn2;
  }));

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  LOG_INFO("Table page content is written to disk");
  bustub_instance->buffer_pool_manager_->FlushPage(first_page_id);

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("System crash before commit");
  delete bustub_instance;

  LOG_INFO("Recovery started..");
  bustub_instance = new BustubInstance("test.db");
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  Tuple old_tuple;
  Tuple old_tuple1;

  txn = bustub_instance->transaction_manager_->Begin();

  ASSERT_FALSE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));

  bustub_instance->transaction_manager_->Commit(txn);

  delete bustub_instance;
  delete txn;
  delete log_recovery;
  delete test_table;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_MixedTestWithMultipleTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();
  bustub_instance->transaction_manager_->Commit(txn);

  RID rid;
  RID rid1;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  // Create 3 tuples
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_2 = tuple.GetValue(&schema, 2);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val1_4 = tuple1.GetValue(&schema, 4);
  auto val1_2 = tuple1.GetValue(&schema, 2);
  auto val1_0 = tuple1.GetValue(&schema, 0);

  auto val2_1 = tuple2.GetValue(&schema, 1);
  auto val2_2 = tuple2.GetValue(&schema, 2);
  auto val3_1 = tuple3.GetValue(&schema, 1);
  auto val3_2 = tuple3.GetValue(&schema, 2);

  const int num_threads = 2;
  std::vector<std::thread> threads;

  // thread 0
  threads.emplace_back(std::thread([&test_table, &tuple, &tuple1, bustub_instance, &rid]() {
    Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
    ASSERT_TRUE(test_table->UpdateTuple(tuple1, rid, txn1));
    bustub_instance->transaction_manager_->Commit(txn1);
    delete txn1;
  }));

  // thread 1
  threads.emplace_back(std::thread([&test_table, &tuple2, &tuple3, bustub_instance, &rid1]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid1, txn2));
    ASSERT_TRUE(test_table->UpdateTuple(tuple3, rid1, txn2));
    delete txn2;
  }));

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown system ...");
  delete bustub_instance;

  LOG_INFO("Bringing system back up ...");

  bustub_instance = new BustubInstance("test.db");
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  Tuple old_tuple;
  Tuple old_tuple1;

  txn = bustub_instance->transaction_manager_->Begin();

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));

  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val1_0), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 2).CompareEquals(val1_2), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val1_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpFalse);
  ASSERT_EQ(old_tuple.GetValue(&schema, 2).CompareEquals(val_2), CmpBool::CmpFalse);
  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpFalse);

  ASSERT_FALSE(test_table->GetTuple(rid1, &old_tuple1, txn));

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Grab tuple");

  delete bustub_instance;
  delete txn;
  delete log_recovery;
  delete test_table;
  LOG_INFO("Tore down the system");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_GroupCommitTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();
  bustub_instance->transaction_manager_->Commit(txn);

  log_timeout = std::chrono::seconds(5);

  RID rid;
  RID rid1;
  RID rid2;
  RID rid3;
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  // Create 3 tuples
  const Tuple tuple = ConstructTuple(&schema);
  const Tuple tuple1 = ConstructTuple(&schema);
  const Tuple tuple2 = ConstructTuple(&schema);
  const Tuple tuple3 = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val1_4 = tuple1.GetValue(&schema, 4);
  auto val2_4 = tuple2.GetValue(&schema, 4);
  auto val3_4 = tuple3.GetValue(&schema, 4);

  const int num_threads = 4;
  std::vector<std::thread> threads;

  // thread 0
  threads.emplace_back(std::thread([&test_table, &tuple, bustub_instance, &rid]() {
    Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
    bustub_instance->transaction_manager_->Commit(txn1);
    delete txn1;
  }));

  // thread 1
  threads.emplace_back(std::thread([&test_table, &tuple1, bustub_instance, &rid1]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple1, &rid1, txn2));
    bustub_instance->transaction_manager_->Commit(txn2);
    delete txn2;
  }));

  // thread 2
  threads.emplace_back(std::thread([&test_table, &tuple2, bustub_instance, &rid2]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple2, &rid2, txn2));
    bustub_instance->transaction_manager_->Commit(txn2);
    delete txn2;
  }));

  // thread 3
  threads.emplace_back(std::thread([&test_table, &tuple3, bustub_instance, &rid3]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    ASSERT_TRUE(test_table->InsertTuple(tuple3, &rid3, txn2));
    bustub_instance->transaction_manager_->Commit(txn2);
    delete txn2;
  }));

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);
  ASSERT_LE(bustub_instance->disk_manager_->GetNumFlushes(), 2);
  // Note: one for the create table commit, one for 4 txn's group commit

  LOG_INFO("Shutdown system ...");
  // shutdown System
  delete bustub_instance;

  LOG_INFO("Bringing system back up ...");

  bustub_instance = new BustubInstance("test.db");
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  Tuple old_tuple;
  Tuple old_tuple1;
  Tuple old_tuple2;
  Tuple old_tuple3;

  txn = bustub_instance->transaction_manager_->Begin();

  ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
  ASSERT_TRUE(test_table->GetTuple(rid1, &old_tuple1, txn));
  ASSERT_TRUE(test_table->GetTuple(rid2, &old_tuple2, txn));
  ASSERT_TRUE(test_table->GetTuple(rid3, &old_tuple3, txn));

  ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple1.GetValue(&schema, 4).CompareEquals(val1_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple2.GetValue(&schema, 4).CompareEquals(val2_4), CmpBool::CmpTrue);
  ASSERT_EQ(old_tuple3.GetValue(&schema, 4).CompareEquals(val3_4), CmpBool::CmpTrue);

  bustub_instance->transaction_manager_->Commit(txn);

  delete bustub_instance;
  delete test_table;
  delete txn;
  delete log_recovery;
  LOG_INFO("Tore down the system");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_BufferPoolSyncFlushTestWithOneTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();

  log_timeout = std::chrono::seconds(5);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  std::vector<RID> rids;
  const Tuple tuple = ConstructTuple(&schema);

  for (int i = 0; i < 200; i++) {
    RID rid;
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn));
    rids.emplace_back(rid);
  }

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_2 = tuple.GetValue(&schema, 2);
  auto val_3 = tuple.GetValue(&schema, 3);

  bustub_instance->transaction_manager_->Commit(txn);
  LOG_INFO("Commit txn");

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Recovery started..");
  bustub_instance = new BustubInstance("test.db");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  for (auto rid : rids) {
    Tuple old_tuple;
    ASSERT_TRUE(test_table->GetTuple(rid, &old_tuple, txn));
    ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 2).CompareEquals(val_2), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 3).CompareEquals(val_3), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);
  }

  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_BufferPoolSyncFlushTestWithMultipleTxn) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();
  bustub_instance->transaction_manager_->Commit(txn);

  log_timeout = std::chrono::seconds(5);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  std::vector<RID> rids_committed;
  std::vector<RID> rids_aborted;
  const Tuple tuple = ConstructTuple(&schema);
  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_2 = tuple.GetValue(&schema, 2);
  auto val_3 = tuple.GetValue(&schema, 3);

  const int num_threads = 2;
  std::vector<std::thread> threads;

  // thread 0
  threads.emplace_back(std::thread([&test_table, &tuple, &rids_committed, bustub_instance]() {
    Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
    for (int i = 0; i < 100; i++) {
      RID rid;
      ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
      rids_committed.emplace_back(rid);
    }
    bustub_instance->transaction_manager_->Commit(txn1);
    delete txn1;
  }));

  // thread 1
  threads.emplace_back(std::thread([&test_table, &tuple, &rids_aborted, bustub_instance]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    for (int i = 0; i < 100; i++) {
      RID rid;
      ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn2));
      rids_aborted.emplace_back(rid);
    }
    bustub_instance->transaction_manager_->Abort(txn2);
    delete txn2;
  }));

  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  delete txn;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Recovery started..");
  bustub_instance = new BustubInstance("test.db");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);

  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  for (auto i : rids_committed) {
    Tuple old_tuple;
    ASSERT_TRUE(test_table->GetTuple(i, &old_tuple, txn));
    ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 2).CompareEquals(val_2), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 3).CompareEquals(val_3), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);
  }

  TableIterator itr = test_table->Begin(txn);
  int size = 0;
  while (itr != test_table->End()) {
    ++itr;
    size++;
  }
  ASSERT_EQ(rids_committed.size(), size);

  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_CheckpointDurabilityTest) {
#undef LOG_BUFFER_SIZE
#define LOG_BUFFER_SIZE PAGE_SIZE

#undef BUFFER_POOL_SIZE
#define BUFFER_POOL_SIZE 100
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  EXPECT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  EXPECT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  bustub_instance->transaction_manager_->Commit(txn);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  Tuple tuple = ConstructTuple(&schema);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_2 = tuple.GetValue(&schema, 2);
  auto val_3 = tuple.GetValue(&schema, 3);
  auto val_4 = tuple.GetValue(&schema, 4);

  // set log time out very high so that flush doesn't happen before checkpoint is performed
  log_timeout = std::chrono::seconds(15);

  // insert a ton of tuples
  Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
  for (int i = 0; i < 500; i++) {
    RID rid;
    EXPECT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
  }
  bustub_instance->transaction_manager_->Commit(txn1);

  // Do checkpoint
  bustub_instance->checkpoint_manager_->BeginCheckpoint();
  bustub_instance->checkpoint_manager_->EndCheckpoint();

  // Hacky
  Page *pages = dynamic_cast<BufferPoolManager*>(bustub_instance->buffer_pool_manager_)->GetPages();
  size_t pool_size = bustub_instance->buffer_pool_manager_->GetPoolSize();

  // make sure that all pages in the buffer pool are marked as non-dirty
  bool all_pages_clean = true;
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID && page->IsDirty()) {
      all_pages_clean = false;
      break;
    }
  }
  EXPECT_TRUE(all_pages_clean);

  // compare each page in the buffer pool to that page's
  // data on disk. ensure they match after the checkpoint
  bool all_pages_match = true;
  auto *disk_data = new char[PAGE_SIZE];
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID) {
      bustub_instance->disk_manager_->ReadPage(page_id, disk_data);
      if (std::memcmp(disk_data, page->GetData(), PAGE_SIZE) != 0) {
        all_pages_match = false;
        break;
      }
    }
  }

  EXPECT_TRUE(all_pages_match);
  delete[] disk_data;

  // Verify all committed transactions flushed to disk
  lsn_t persistent_lsn = bustub_instance->log_manager_->GetPersistentLSN();
  lsn_t next_lsn = bustub_instance->log_manager_->GetNextLSN();
  EXPECT_EQ(persistent_lsn, (next_lsn - 1));

  // verify log was flushed and each page's LSN <= persistent lsn
  bool all_pages_lte = true;
  for (size_t i = 0; i < pool_size; i++) {
    Page *page = &pages[i];
    page_id_t page_id = page->GetPageId();

    if (page_id != INVALID_PAGE_ID && page->GetLSN() > persistent_lsn) {
      all_pages_lte = false;
      break;
    }
  }

  EXPECT_TRUE(all_pages_lte);

  delete txn;
  delete txn1;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_CheckpointConcurrencyTest) {
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  EXPECT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  // set log time out very high so that flush doesn't happen before checkpoint is performed
  log_timeout = std::chrono::seconds(15);

  bustub_instance->log_manager_->RunFlushThread();
  EXPECT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  bustub_instance->transaction_manager_->Commit(txn);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  Tuple tuple = ConstructTuple(&schema);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_2 = tuple.GetValue(&schema, 2);
  auto val_3 = tuple.GetValue(&schema, 3);
  auto val_4 = tuple.GetValue(&schema, 4);

  lsn_t pre_txn_lsn = bustub_instance->log_manager_->GetNextLSN();

  // insert many tuples
  Transaction *txn1 = bustub_instance->transaction_manager_->Begin();
  for (int i = 0; i < 200; i++) {
    RID rid;
    EXPECT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
  }
  bustub_instance->transaction_manager_->Commit(txn1);

  lsn_t post_txn_lsn = bustub_instance->log_manager_->GetNextLSN();
  EXPECT_TRUE(pre_txn_lsn < post_txn_lsn);

  lsn_t pre_checkpoint_lsn = bustub_instance->log_manager_->GetNextLSN();

  // Do checkpoint, but stay blocked
  bustub_instance->checkpoint_manager_->BeginCheckpoint();

  // start thread to run transactions here
  std::thread thread_1([&test_table, &tuple, bustub_instance]() {
    Transaction *txn2 = bustub_instance->transaction_manager_->Begin();
    for (int i = 0; i < 10; i++) {
      RID rid;
      EXPECT_TRUE(test_table->InsertTuple(tuple, &rid, txn2));
    }
    bustub_instance->transaction_manager_->Commit(txn2);
    delete txn2;
  });

  std::chrono::milliseconds sleep_duration(100);
  std::this_thread::sleep_for(sleep_duration);

  // make sure no transactions proceeded before EndCheckpoint() is called
  lsn_t during_checkpoint_lsn = bustub_instance->log_manager_->GetNextLSN();
  EXPECT_EQ(during_checkpoint_lsn, pre_checkpoint_lsn);

  // end checkpoint, make sure transactions resume
  bustub_instance->checkpoint_manager_->EndCheckpoint();

  thread_1.join();

  lsn_t post_checkpoint_lsn = bustub_instance->log_manager_->GetNextLSN();
  EXPECT_TRUE(post_checkpoint_lsn > during_checkpoint_lsn);

  delete txn;
  delete txn1;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("See if timeout works and log buffer is flushed to disk");
  ASSERT_GE(bustub_instance->disk_manager_->GetNumFlushes(), 1);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

// NOLINTNEXTLINE
TEST(RecoveryTest, DISABLED_TestAsyncLogging) {
  // test when disk manager is flushing buffer to disk,
  // tuple can be inserted

#undef LOG_BUFFER_SIZE
#define LOG_BUFFER_SIZE PAGE_SIZE

#undef BUFFER_POOL_SIZE
#define BUFFER_POOL_SIZE 100
  log_timeout = std::chrono::seconds(1);
  remove("test.db");
  remove("test.log");
  BustubInstance *bustub_instance = new BustubInstance("test.db");

  ASSERT_FALSE(enable_logging);
  LOG_INFO("Skip system recovering...");

  bustub_instance->log_manager_->RunFlushThread();
  ASSERT_TRUE(enable_logging);
  LOG_INFO("System logging thread running...");

  LOG_INFO("Create a test table");
  Transaction *txn = bustub_instance->transaction_manager_->Begin();
  auto *test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                                   bustub_instance->log_manager_, txn);
  page_id_t first_page_id = test_table->GetFirstPageId();
  bustub_instance->transaction_manager_->Commit(txn);

  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};

  std::vector<RID> rids;
  const Tuple tuple = ConstructTuple(&schema);

  auto val_4 = tuple.GetValue(&schema, 4);
  auto val_0 = tuple.GetValue(&schema, 0);
  auto val_1 = tuple.GetValue(&schema, 1);
  auto val_2 = tuple.GetValue(&schema, 2);
  auto val_3 = tuple.GetValue(&schema, 3);

  Transaction *txn1 = bustub_instance->transaction_manager_->Begin();

  std::promise<void> flush_log_p;
  std::future<void> flush_log_f = flush_log_p.get_future();
  bustub_instance->disk_manager_->SetFlushLogFuture(&flush_log_f);

  for (int i = 0; i < 700; i++) {
    if (bustub_instance->disk_manager_->GetFlushState() && bustub_instance->disk_manager_->HasFlushLogFuture()) {
      flush_log_p.set_value();
      bustub_instance->disk_manager_->SetFlushLogFuture(nullptr);
    }

    RID rid;
    ASSERT_TRUE(test_table->InsertTuple(tuple, &rid, txn1));
    rids.emplace_back(rid);
  }
  bustub_instance->transaction_manager_->Commit(txn1);
  LOG_INFO("Txn Commited");
  delete txn;
  delete txn1;
  delete test_table;

  LOG_INFO("SLEEPING for 2s");
  std::chrono::seconds timespan(2);
  std::this_thread::sleep_for(timespan);

  LOG_INFO("Shutdown System");
  delete bustub_instance;

  LOG_INFO("Recovery started..");
  bustub_instance = new BustubInstance("test.db");
  auto *log_recovery = new LogRecovery(bustub_instance->disk_manager_, bustub_instance->buffer_pool_manager_);
  ASSERT_FALSE(enable_logging);

  log_recovery->Redo();
  LOG_INFO("Redo underway...");
  log_recovery->Undo();
  LOG_INFO("Undo underway...");

  txn = bustub_instance->transaction_manager_->Begin();
  test_table = new TableHeap(bustub_instance->buffer_pool_manager_, bustub_instance->lock_manager_,
                             bustub_instance->log_manager_, first_page_id);

  for (auto rid : rids) {
    Tuple old_tuple;
    assert(test_table->GetTuple(rid, &old_tuple, txn) == 1);
    ASSERT_EQ(old_tuple.GetValue(&schema, 0).CompareEquals(val_0), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 1).CompareEquals(val_1), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 2).CompareEquals(val_2), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 3).CompareEquals(val_3), CmpBool::CmpTrue);
    ASSERT_EQ(old_tuple.GetValue(&schema, 4).CompareEquals(val_4), CmpBool::CmpTrue);
  }

  bustub_instance->transaction_manager_->Commit(txn);
  delete txn;
  delete test_table;
  delete log_recovery;

  delete bustub_instance;
  LOG_INFO("Tearing down the system..");
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
