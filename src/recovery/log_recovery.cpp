//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(int pos, LogRecord *log_record) {
  int buffer_size = LOG_BUFFER_SIZE;
  if (pos + 20 > buffer_size) {
    return false;
  }
  // std::cout<<pos<<std::endl;
  // 先获取头部信息,再根据头部信息添加
  memcpy(reinterpret_cast<void *>(log_record), reinterpret_cast<void *>(log_buffer_ + pos), 20);
  // LOG_DEBUG("start pos%d cur_offset:%d log size %d", pos, offset_, log_record->GetSize());
  if (log_record->GetSize() == 0 || pos + log_record->GetSize() > buffer_size) {
    return false;
  }
  pos += 20;
  switch (log_record->log_record_type_) {
    case LogRecordType::INSERT: {
      memcpy(&log_record->insert_rid_, log_buffer_ + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->insert_tuple_.DeserializeFrom(log_buffer_ + pos);
      break;
    };
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
    case LogRecordType::APPLYDELETE: {
      memcpy(&log_record->delete_rid_, log_buffer_ + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->delete_tuple_.DeserializeFrom(log_buffer_ + pos);
      break;
    };
    case LogRecordType::UPDATE: {
      memcpy(&log_record->update_rid_, log_buffer_ + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->old_tuple_.DeserializeFrom(log_buffer_ + pos);
      // length + data
      pos += sizeof(uint32_t) + log_record->old_tuple_.GetLength();
      log_record->new_tuple_.DeserializeFrom(log_buffer_ + pos);
      break;
    };
    case LogRecordType::NEWPAGE: {
      memcpy(&log_record->prev_page_id_, log_buffer_ + pos, sizeof(page_id_t));
      pos += sizeof(page_id_t);
      memcpy(&log_record->page_id_, log_buffer_ + pos, sizeof(page_id_t));
      break;
    };
    default: {
      break;
    }
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  memset(log_buffer_, 0, BUFFER_POOL_SIZE);
  offset_ = 0;
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
    LogRecord log_record;
    int pre_log_offset = offset_;
    int pos = 0;
    while (DeserializeLogRecord(pos, &log_record)) {
      RedoLog(&log_record);
      pos += log_record.GetSize();
      offset_ += log_record.GetSize();
    }
    // 如果最后一个日志不完整
    if (pre_log_offset == offset_) {
      break;
    }
  }
}
void LogRecovery::RedoLog(LogRecord *log_record) {
  // LOG_DEBUG("redo log %s", log_record->ToString().c_str());
  txn_id_t txn_id = log_record->GetTxnId();
  lsn_t lsn = log_record->GetLSN();
  lsn_mapping_[lsn] = offset_;
  active_txn_[txn_id] = lsn;
  switch (log_record->log_record_type_) {
    // case LogRecordType::BEGIN:{break;};
    // // abort and commit are good txn
    case LogRecordType::ABORT:
    case LogRecordType::COMMIT: {
      active_txn_.erase(txn_id);
      break;
    };
    case LogRecordType::INSERT: {
      TablePage *insert_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->insert_rid_.GetPageId()));
      if (insert_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      bool dirty = false;
      if (insert_page->GetLSN() < lsn) {
        // the rid is new
        // LOG_DEBUG("insert tuple rid: %s size:%d", log_record->insert_rid_.ToString().c_str(),
        //           log_record->insert_tuple_.GetLength());
        insert_page->InsertTuple(log_record->insert_tuple_, &log_record->insert_rid_, nullptr, nullptr, nullptr);
        insert_page->SetLSN(lsn);  // 可以不用重设,因为lsn是为了恢复用的,后面运行的时候产生的lsn肯定比现在大,所以不用管
        dirty = true;
      }
      buffer_pool_manager_->UnpinPage(insert_page->GetPageId(), dirty);
      break;
    };
    case LogRecordType::MARKDELETE: {
      TablePage *delete_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->delete_rid_.GetPageId()));
      if (delete_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      bool dirty = false;
      if (delete_page->GetLSN() < lsn) {
        delete_page->MarkDelete(log_record->delete_rid_, nullptr, nullptr, nullptr);
        delete_page->SetLSN(lsn);
        dirty = true;
      }
      buffer_pool_manager_->UnpinPage(delete_page->GetPageId(), dirty);
      break;
    };
    case LogRecordType::ROLLBACKDELETE: {
      TablePage *delete_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->delete_rid_.GetPageId()));
      if (delete_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      bool dirty = false;
      if (delete_page->GetLSN() < lsn) {
        delete_page->RollbackDelete(log_record->delete_rid_, nullptr, nullptr);
        delete_page->SetLSN(lsn);
        dirty = true;
      }
      buffer_pool_manager_->UnpinPage(delete_page->GetPageId(), dirty);
      break;
    };
    case LogRecordType::APPLYDELETE: {
      TablePage *delete_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->delete_rid_.GetPageId()));
      if (delete_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      bool dirty = false;
      if (delete_page->GetLSN() < lsn) {
        delete_page->ApplyDelete(log_record->delete_rid_, nullptr, nullptr);
        delete_page->SetLSN(lsn);
        dirty = true;
      }
      buffer_pool_manager_->UnpinPage(delete_page->GetPageId(), dirty);
      break;
    };
    case LogRecordType::UPDATE: {
      TablePage *update_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->update_rid_.GetPageId()));
      if (update_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      bool dirty = false;
      if (update_page->GetLSN() < lsn) {
        // LOG_DEBUG("redo update_record rid: %s  old size:%d new size:%d", log_record->update_rid_.ToString().c_str(),
        //           log_record->old_tuple_.GetLength(), log_record->new_tuple_.GetLength());
        update_page->UpdateTuple(log_record->new_tuple_, &log_record->old_tuple_, log_record->update_rid_, nullptr,
                                 nullptr, nullptr);
        update_page->SetLSN(lsn);
        dirty = true;
      }
      buffer_pool_manager_->UnpinPage(update_page->GetPageId(), dirty);
      break;
    };
    case LogRecordType::NEWPAGE: {
      TablePage *cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->page_id_));
      if (cur_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      bool dirty = false;
      if (cur_page->GetLSN() < lsn) {
        if (log_record->prev_page_id_ != INVALID_PAGE_ID) {
          TablePage *pre_page =
              reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->prev_page_id_));
          if (pre_page == nullptr) {
            throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
          }
          pre_page->SetNextPageId(log_record->page_id_);
          buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), true);
        }
        cur_page->Init(cur_page->GetPageId(), PAGE_SIZE, log_record->prev_page_id_, nullptr, nullptr);
        cur_page->SetLSN(lsn);
        dirty = true;
      }
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), dirty);
      break;
    }  // do nothing;
    default: {
      break;
    }
  }
}
bool LogRecovery::FetchLog(int log_offset, LogRecord *log_record) {
  // LOG_DEBUG("log_offset:%d", log_offset);
  int buffer_size = LOG_BUFFER_SIZE;
  int pos = log_offset - offset_;
  if (offset_ == -1 || pos < 0 || !DeserializeLogRecord(pos, log_record)) {
    disk_manager_->ReadLog(log_buffer_, buffer_size, log_offset);
    offset_ = log_offset;
    pos = 0;
    return DeserializeLogRecord(pos, log_record);
  }
  return true;
}

void LogRecovery::UndoLog(LogRecord *log_record) {
  // LOG_DEBUG("undo log:%s", log_record->ToString().c_str());
  lsn_t lsn = log_record->GetLSN();
  switch (log_record->log_record_type_) {
    // case LogRecordType::COMMIT:break;// do nothing
    case LogRecordType::INSERT: {
      TablePage *insert_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->insert_rid_.GetPageId()));
      if (insert_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      assert(insert_page->GetLSN() >= lsn);
      // LOG_DEBUG("delet rid:%s", log_record->insert_rid_.ToString().c_str());
      insert_page->ApplyDelete(log_record->insert_rid_, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(insert_page->GetPageId(), true);
      break;
    };
    case LogRecordType::MARKDELETE: {
      TablePage *delete_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->delete_rid_.GetPageId()));
      if (delete_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      assert(delete_page->GetLSN() >= lsn);
      delete_page->RollbackDelete(log_record->delete_rid_, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(delete_page->GetPageId(), true);
      break;
    };
    case LogRecordType::ROLLBACKDELETE: {
      TablePage *delete_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->delete_rid_.GetPageId()));
      if (delete_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      assert(delete_page->GetLSN() >= lsn);
      delete_page->MarkDelete(log_record->delete_rid_, nullptr, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(delete_page->GetPageId(), true);
      break;
    };
    case LogRecordType::APPLYDELETE: {
      TablePage *delete_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->delete_rid_.GetPageId()));
      if (delete_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      assert(delete_page->GetLSN() >= lsn);
      delete_page->InsertTuple(log_record->delete_tuple_, &log_record->delete_rid_, nullptr, nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(delete_page->GetPageId(), true);
      break;
    };
    case LogRecordType::UPDATE: {
      TablePage *update_page =
          reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->update_rid_.GetPageId()));
      if (update_page == nullptr) {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      }
      assert(update_page->GetLSN() >= lsn);
      update_page->UpdateTuple(log_record->old_tuple_, &log_record->new_tuple_, log_record->update_rid_, nullptr,
                               nullptr, nullptr);
      buffer_pool_manager_->UnpinPage(update_page->GetPageId(), true);
      break;
    }
    case LogRecordType::NEWPAGE: { 
      //do nothing althogh cur_page maybe empty,because there has no public function to judge table_page empty
      // if (log_record->prev_page_id_ != INVALID_PAGE_ID) {
      //   TablePage *pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->prev_page_id_));
      //   // TablePage *cur_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record->page_id_));
      //   if (pre_page == nullptr) {
      //     throw Exception(ExceptionType::OUT_OF_MEMORY, "fetch page error!");
      //   }
      //   if(pre_page->GetNextPageId()==cur_page->GetPageId())
      //   pre_page->SetNextPageId(INVALID_PAGE_ID);
      //   buffer_pool_manager_->UnpinPage(pre_page->GetPageId(), true);
      // }
      break;
    }
    // case LogRecordType::NEWPAGE:break;  // do nothing;do something! do nothing!
    default: {
      break;
    }
  }
}
/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  memset(log_buffer_, 0, BUFFER_POOL_SIZE);
  offset_ = -1;
  for (auto cur_lsn : active_txn_) {
    lsn_t lsn = cur_lsn.second;
    LogRecord log_record;
    while (lsn != INVALID_LSN) {
      int log_offset = lsn_mapping_[lsn];
      assert(FetchLog(log_offset, &log_record));
      UndoLog(&log_record);
      lsn = log_record.GetPrevLSN();
    };
  }
}

}  // namespace bustub
