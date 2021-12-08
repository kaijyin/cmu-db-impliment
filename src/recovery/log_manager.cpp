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

#include "common/logger.h"

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
  flush_thread_=new std::thread(&LogManager::RunThread,this);
}
void LogManager::RunThread() {
    std::future<void> flush_log_f = flush_log_p.get_future();
   while (enable_logging) {
    flush_log_f.wait_for(std::chrono::seconds(log_timeout));
    latch_.lock();
    if(offset_==0){
        // tiny upgrade
        latch_.unlock();
        block_txn_cv_.notify_all();
        continue;
    }
    LOG_DEBUG("flush log buffer,size:%d",offset_);
    char*tmp=log_buffer_;
    log_buffer_=flush_buffer_;
    flush_buffer_=tmp;
    lsn_t cur_lsn=next_lsn_-1;
    int lenth=offset_;
    memset(log_buffer_,0,LOG_BUFFER_SIZE);
    offset_=0;
    latch_.unlock();
    disk_manager_->WriteLog(flush_buffer_,lenth);
    persistent_lsn_=cur_lsn;
    block_txn_cv_.notify_all();
  }
}
void LogManager::FlushBuffer(){
    try{
        flush_log_p.set_value();
    }catch (std::exception& e) {
        std::cout << "[exception caught: " << e.what() << "]\n";
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

void LogManager::WaitFlush() {
    std::unique_lock lock(latch_);
    block_txn_cv_.wait(lock);
}
/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord *log_record) {
  latch_.lock();
  while(offset_+log_record->GetSize()>LOG_BUFFER_SIZE){
      latch_.unlock();
     FlushBuffer();
     latch_.lock();
  }
//   LOG_DEBUG("append log %s",log_record->ToString().c_str());
  log_record->lsn_ = next_lsn_++;
  memcpy(log_buffer_ + offset_, log_record, LogRecord::HEADER_SIZE);
  uint32_t pos = offset_ + LogRecord::HEADER_SIZE;
  switch(log_record->log_record_type_){
      case LogRecordType::INSERT:{
        //   LOG_DEBUG("insert tuple LOG rid: %s size:%d",log_record->insert_rid_.ToString().c_str(),
        //   log_record->insert_tuple_.GetLength());
          memcpy(log_buffer_ + pos, &log_record->insert_rid_, sizeof(RID));
          pos += sizeof(RID);
          log_record->insert_tuple_.SerializeTo(log_buffer_ + pos);
          break;
      };
      case LogRecordType::MARKDELETE:
      case LogRecordType::ROLLBACKDELETE:
      case LogRecordType::APPLYDELETE:{
          memcpy(log_buffer_ + pos, &log_record->delete_rid_, sizeof(RID));
          pos += sizeof(RID);
          log_record->delete_tuple_.SerializeTo(log_buffer_ + pos);
          break;
      };
      case LogRecordType::UPDATE:{
        //   LOG_DEBUG("append update_record rid: %s  old size:%d new size:%d", 
        // log_record->update_rid_.ToString().c_str(),
        //           log_record->old_tuple_.GetLength(),log_record->new_tuple_.GetLength());
          memcpy(log_buffer_ + pos, &log_record->update_rid_, sizeof(RID));
          pos += sizeof(RID);
          log_record->old_tuple_.SerializeTo(log_buffer_ + pos);
          // size + data
          pos+=sizeof(uint32_t)+log_record->old_tuple_.GetLength();
          log_record->new_tuple_.SerializeTo(log_buffer_ + pos);
          break;
      };
      case LogRecordType::NEWPAGE:{
          memcpy(log_buffer_ + pos, &log_record->prev_page_id_, sizeof(page_id_t));
          pos+=sizeof(page_id_t);
          memcpy(log_buffer_ + pos, &log_record->page_id_, sizeof(page_id_t));
          break;
      };
      default:;
  }
  offset_+=log_record->GetSize();
  latch_.unlock();
  return log_record->lsn_;
 }

}  // namespace bustub
