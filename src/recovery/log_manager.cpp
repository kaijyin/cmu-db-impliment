//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_manager.cpp
//
// Identification: src/recovery/log_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_manager.h"

namespace bustub {

/*
 * set enable_logging = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when timeout or the log buffer is full or buffer
 * pool manager wants to force flush (it only happens when the flushed page has
 * a larger LSN than persistent LSN)
 *
 * This thread runs forever until system shutdown/StopFlushThread
 */
void LogManager::RunFlushThread() {
  enable_logging=true;
  flush_thread_=new std::thread(&RunThread,this);
}
void LogManager::RunThread() {
   while (enable_logging) {
    std::this_thread::sleep_for(log_timeout);
    FlushBuffer();
   }
}
/*
 * Stop and join the flush thread, set enable_logging = false
 */
void LogManager::StopFlushThread() {
   enable_logging=false;
   flush_thread_->join();
   delete flush_thread_;
}
void LogManager::FlushBufferAndWait(){
    buffer_latch_.lock();
    latch_.lock();
    char*tmp=log_buffer_;
    log_buffer_=flush_buffer_;
    flush_buffer_=tmp;
    lsn_t cur_lsn=next_lsn_-1;
    int lenth=offset_;
    offset_=0;
    latch_.unlock();
    std::thread t(&LogManager::BufferToDisk,this,cur_lsn,lenth);
    t.join();
}
void LogManager::FlushBuffer() {
    buffer_latch_.lock();
    latch_.lock();
    char*tmp=log_buffer_;
    log_buffer_=flush_buffer_;
    flush_buffer_=tmp;
    lsn_t cur_lsn=next_lsn_-1;
    int lenth=offset_;
    offset_=0;
    latch_.unlock();
    std::thread t(&LogManager::BufferToDisk,this,cur_lsn,lenth);
    t.detach();
}
void LogManager::BufferToDisk(lsn_t cur_lsn,int lenth){
   disk_manager_->WriteLog(flush_buffer_,lenth);
   persistent_lsn_=cur_lsn;
   buffer_latch_.unlock();
   block_txn_cv_.notify_all();
}
void LogManager::WaitFlush() {
    std::unique_lock lock(latch_);
    block_txn_cv_.wait(lock);
}
/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  latch_.lock();
  if(offset_+log_record->GetSize()>LOG_BUFFER_SIZE){
     FlushBuffer();
  }
  log_record->lsn_ = next_lsn_++;
  memcpy(log_buffer_ + offset_, log_record, LogRecord::HEADER_SIZE);
  uint32_t pos = offset_ + LogRecord::HEADER_SIZE;
  switch(log_record->log_record_type_){
      case LogRecordType::INSERT:{
          memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
          pos += sizeof(RID);
          log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
      }
      case LogRecordType::MARKDELETE:
      case LogRecordType::ROLLBACKDELETE:
      case LogRecordType::APPLYDELETE:{
          memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
          pos += sizeof(RID);
          log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
      };
      case LogRecordType::UPDATE:{
          memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
          pos += sizeof(RID);
          log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
          // size + data
          pos+=sizeof(uint32_t)+log_record->old_tuple_.GetLength();
          log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
      };
      case LogRecordType::NEWPAGE:{
          memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
          pos+=sizeof(page_id_t);
          memcpy(log_buffer_ + pos, &log_record->page_id_, sizeof(page_id_t));
      };
  }
  offset_+=log_record->GetSize();
  latch_.unlock();
  return log_record->lsn_;
 }

}  // namespace bustub
