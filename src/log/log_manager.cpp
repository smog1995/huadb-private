#include "log/log_manager.h"
#include <memory>
#include "common/constants.h"
#include "iostream"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "log/log_record.h"
#include "log/log_records/log_records.h"
#include "storage/buffer_pool.h"

namespace huadb {

LogManager::LogManager(Disk &disk, TransactionManager &transaction_manager, lsn_t next_lsn)
    : disk_(disk), transaction_manager_(transaction_manager), next_lsn_(next_lsn), flushed_lsn_(next_lsn - 1) {}

void LogManager::SetBufferPool(std::shared_ptr<BufferPool> buffer_pool) { buffer_pool_ = std::move(buffer_pool); }

void LogManager::SetCatalog(std::shared_ptr<Catalog> catalog) { catalog_ = std::move(catalog); }

lsn_t LogManager::GetNextLSN() { return next_lsn_; }

void LogManager::Clear() { log_buffer_.clear(); }

void LogManager::Flush() { Flush(NULL_LSN); }

void LogManager::SetDirty(oid_t oid, pageid_t page_id, lsn_t lsn) {
  if (dpt_.find({oid, page_id}) == dpt_.end()) { // 该脏页第一条日志
    dpt_[{oid, page_id}] = lsn;
  }
}

lsn_t LogManager::AppendInsertLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t offset,
                                  db_size_t size, char *new_record) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendInsertLog)");
  }
  auto log = std::make_shared<InsertLog>(xid, att_[xid], oid, page_id, slot_id, offset, size, new_record);
  lsn_t lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  log->SetLSN(lsn);
  att_[xid] = lsn;
  log_buffer_.push_back(std::move(log));
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendDeleteLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id) {
  // std::cout << "写入事务删除日志, xid:" << xid << std::endl;
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendDeleteLog)");
  }
  auto log = std::make_shared<DeleteLog>(xid, att_[xid], oid, page_id, slot_id);
  lsn_t lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  log->SetLSN(lsn);
  att_[xid] = lsn;
  log_buffer_.push_back(std::move(log));
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendNewPageLog(xid_t xid, oid_t oid, pageid_t prev_page_id, pageid_t page_id) {
  // std::cout << "写入事务新页日志, xid:" << xid << std::endl;
  if (xid != DDL_XID && att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendNewPageLog)");
  }
  std::shared_ptr<NewPageLog> log;
  if (xid == DDL_XID) {
    log = std::make_shared<NewPageLog>(xid, NULL_LSN, oid, prev_page_id, page_id);
  } else {
    log = std::make_shared<NewPageLog>(xid, att_[xid], oid, prev_page_id, page_id);
  }
  lsn_t lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  log->SetLSN(lsn);
  if (xid != DDL_XID) {
    att_[xid] = lsn;
  }
  log_buffer_.push_back(std::move(log));
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  if (prev_page_id != NULL_PAGE_ID && dpt_.find({oid, prev_page_id}) == dpt_.end()) {
    dpt_[{oid, prev_page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendBeginLog(xid_t xid) {
  std::cout << "写入事务开始日志,xid:" << xid << std::endl; 
  if (att_.find(xid) != att_.end()) {
    throw DbException(std::to_string(xid) + " already exists in att");
  }
  auto log = std::make_shared<BeginLog>(xid, NULL_LSN);
  lsn_t lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  att_[xid] = lsn;
  log->SetLSN(lsn);
  log_buffer_.push_back(std::move(log));
  return lsn;
}

lsn_t LogManager::AppendCommitLog(xid_t xid) {
  // std::cout << "写入事务提交日志, xid:" << xid << std::endl;
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendCommitLog)");
  }
  auto log = std::make_shared<CommitLog>(xid, att_[xid]);
  lsn_t lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  log->SetLSN(lsn);
  log_buffer_.push_back(std::move(log));
  //  提交需要刷磁盘 (事务只有将提交记录的日志落盘才算成功提交，WAL机制)
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::AppendRollbackLog(xid_t xid) {
  // std::cout << "写入事务回滚日志, xid:" << xid << std::endl;
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendRollbackLog)");
  }
  auto log = std::make_shared<RollbackLog>(xid, att_[xid]);
  lsn_t lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  log->SetLSN(lsn);
  log_buffer_.push_back(std::move(log));
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

//  检查点恢复，因为是在线状态进行检查点保存，所以当时正在进行的事务也被保存了
lsn_t LogManager::Checkpoint(bool async) {
  auto log = std::make_shared<BeginCheckpointLog>(NULL_XID, NULL_LSN);
  lsn_t begin_lsn = next_lsn_;
  next_lsn_ += log->GetSize();
  log->SetLSN(begin_lsn);
  log_buffer_.push_back(std::move(log));

  auto end_checkpoint_log = std::make_shared<EndCheckpointLog>(NULL_XID, NULL_LSN, att_, dpt_);
  lsn_t end_lsn = next_lsn_;
  next_lsn_ += end_checkpoint_log->GetSize();
  end_checkpoint_log->SetLSN(end_lsn);
  log_buffer_.push_back(std::move(end_checkpoint_log));
  Flush(end_lsn); // 此刻活跃事务所做的操作日志也一同被刷到磁盘 
  std::ofstream out(MASTER_RECORD_NAME);
  out << begin_lsn;
  out.close();
  return end_lsn;
}

void LogManager::FlushPage(oid_t table_oid, pageid_t page_id, lsn_t page_lsn) {
  Flush(page_lsn);
  dpt_.erase({table_oid, page_id});
}

void LogManager::Rollback(xid_t xid) {
  // 在 att_ 中查找事务 xid 的最后一条日志的 lsn
  // 依次获取 lsn 的 prev_lsn_，直到 NULL_LSN
  // 根据 lsn 和 flushed_lsn_ 的大小关系，判断日志在 buffer 中还是在磁盘中
  // 若日志在 buffer 中，通过 log_buffer_ 获取日志
  // 若日志在磁盘中，通过 disk_ 读取日志
  // 调用日志的 Undo 函数
  // LAB 2 BEGIN
  // std::cout << "调用回滚,事务为：" << xid << std::endl;
  lsn_t lsn = att_[xid];
  lsn_t prev_lsn = lsn;
  lsn_t flushed_lsn =flushed_lsn_;
  std::shared_ptr<LogRecord> log_record;
  bool rollback_finish = false;
  if (redo_count_ > 0) {  
    if (disk_.FileExists(MASTER_RECORD_NAME)) {
      std::ifstream in(MASTER_RECORD_NAME);
      in >> flushed_lsn;
      in.close();
    }
  }
  size_t last_record_size = 0;
  if (lsn <= flushed_lsn) {
    last_record_size = flushed_lsn - lsn;
  }
  while (!rollback_finish) {
    // std::cout << flushed_lsn<<std::endl;
    if (prev_lsn <= flushed_lsn) { // 不在日志缓冲区中
      // std::cout << "日志不在内存" << std::endl;
      size_t record_size;
      if (last_record_size != 0 || prev_lsn == lsn) {
        record_size = flushed_lsn - lsn;
      } else {
        record_size = lsn - prev_lsn;
      }
      // std::cout << "日志大小：" << record_size << std::endl;
      char* log = new char[record_size];
      disk_.ReadLog(lsn, record_size, log);
      log_record = LogRecord::DeserializeFrom(log);
      lsn = prev_lsn;
      prev_lsn = log_record->GetPrevLSN(); 
      delete[] log;
      if (log_record->GetType() == LogType::INSERT || log_record->GetType() == LogType::DELETE 
          || log_record->GetType() == LogType::NEW_PAGE) {
        log_record->Undo(*buffer_pool_, *catalog_, *this, lsn, prev_lsn);
      } else if (log_record->GetType() == LogType::BEGIN) {
        rollback_finish = true;
        // std::cout << "回滚结束" << std::endl;
      }
      
    } else {
      // std::cout << "日志在内存中" << std::endl;
      auto iterator = log_buffer_.cbegin();
      for (; iterator != log_buffer_.cend(); iterator++) {
        log_record = *iterator;
        if (log_record->GetLSN() == lsn) {
          prev_lsn = log_record->GetPrevLSN();
          // std::cout << " prevlsn:" << prev_lsn <<std::endl;
          // std::cout << "日志类型为：" << (int)log_record->GetType() << std::endl;
          if (log_record->GetType() == LogType::INSERT || log_record->GetType() == LogType::DELETE
           || log_record->GetType() == LogType::NEW_PAGE) {
            log_record->Undo(*buffer_pool_, *catalog_, *this, lsn, prev_lsn);
            // std::cout <<"undo" <<std::endl;
          } else if (log_record->GetType() == LogType::BEGIN) {
            rollback_finish = true;
            // std::cout << "回滚结束" << std::endl;
          }
          lsn = prev_lsn;
          break;
        }
      }
    }
  }
  
}

void LogManager::Recover() {
  // std::cout <<"ARIES回复" << std::endl;
  Analyze();
  Redo();
  Undo();
  // std::cout << "recover结束" << std::endl;
}

void LogManager::IncrementRedoCount() { redo_count_++; }

uint32_t LogManager::GetRedoCount() const { return redo_count_; }


//  将所有在lsn前的且仍在日志缓冲区的记录全部写入磁盘

void LogManager::Flush(lsn_t lsn) {
  size_t last_log_size = 0;
  lsn_t last_log_lsn = flushed_lsn_;
  auto iterator = log_buffer_.cbegin();
  for (; iterator != log_buffer_.cend(); iterator++) {
    const auto &log_record = *iterator;
    
    if (lsn != NULL_LSN && log_record->GetLSN() > lsn) {
      break;
    }
    last_log_size = log_record->GetSize();
    char *log = new char[last_log_size];

  //  serializeTo并不会把日志的lsn成员变量也序列化，所以写入磁盘的也是不带有lsn的
    log_record->SerializeTo(log);  
    disk_.WriteLog(log_record->GetLSN(), last_log_size, log);
    delete[] log;
    last_log_lsn = log_record->GetLSN();
  }
  log_buffer_.erase(log_buffer_.begin(), iterator);
  std::ofstream out(NEXT_LSN_NAME);
  //  STEAL 允许事务未提交就已经将部分操作日志落盘
  if (lsn == NULL_LSN && last_log_lsn > flushed_lsn_) {
    flushed_lsn_ = last_log_lsn;
  //  事务提交时flush
  } else if (lsn > flushed_lsn_) {
    flushed_lsn_ = lsn;
  }
  out << (flushed_lsn_ + last_log_size);
  out.flush();
}

void LogManager::Analyze() {
  // 恢复 Master Record 等元信息
  // 恢复故障时正在使用的数据库
  std::cout << "analyze" <<std::endl;
  std::ifstream in(NEXT_LSN_NAME);
  lsn_t next_lsn;
  in >> next_lsn;
  next_lsn_ = next_lsn;
  flushed_lsn_ = next_lsn_ - 1;
  in.close();
  lsn_t checkpoint_lsn = 0;
  if (disk_.FileExists(MASTER_RECORD_NAME)) {
    std::ifstream in(MASTER_RECORD_NAME);
    in >> checkpoint_lsn;
    in.close();
  }
  //  根据 Checkpoint 日志恢复脏页表、活跃事务表等元信息
  //  LAB 2 BEGIN
  // bool analyze_flag = false;
  //第一步，将所有日志写到日志缓冲区
  //  开一个很大的内存空间（偷懒）
  size_t all_log_record_size = flushed_lsn_ - checkpoint_lsn;
  char* log_data = new char[all_log_record_size];
  // std::cout << "all_log_record_size" << all_log_record_size << std::endl;
  disk_.ReadLog(checkpoint_lsn, all_log_record_size, log_data);
  size_t log_data_offset = 0;
  bool analyze_flag = false;
  while (!analyze_flag) {
    auto log_record = LogRecord::DeserializeFrom(log_data + log_data_offset);
    // 本实验在开始检查点日志和结束检查点日志之间不存在其他日志
    switch (log_record->GetType()) {
      case LogType::INSERT:
      case LogType::DELETE:
      case LogType::NEW_PAGE:
      case LogType::BEGIN:
      case LogType::COMMIT:
      case LogType::ROLLBACK:
      case LogType::BEGIN_CHECKPOINT:
        break;
      case LogType::END_CHECKPOINT:
        dpt_ = reinterpret_cast<EndCheckpointLog*>(log_record.get())->GetDPT();
        att_ = reinterpret_cast<EndCheckpointLog*>(log_record.get())->GetATT();
        analyze_flag = true;
        break;
      default:
        throw DbException("Unknown log type");
    }
    log_data_offset += log_record->GetSize();
    // std::cout << " log_data_offset" <<log_data_offset;
  }
  // std::cout << std::endl;
  delete[] log_data;
}

void LogManager::Redo() {
  IncrementRedoCount();
  // std::cout << "调用redo" << std::endl;
  // 正序读取日志，调用日志记录的 Redo 函数
  // LAB 2 BEGIN
  // 需要从脏页表中选取最小的rec lsn开始重做
  lsn_t checkpoint_lsn = 0;
  if (disk_.FileExists(MASTER_RECORD_NAME)) {
    std::ifstream in(MASTER_RECORD_NAME);
    in >> checkpoint_lsn;
    in.close();
  }
  auto iterator = dpt_.cbegin();
  lsn_t min_redo_lsn = checkpoint_lsn;
  for (; iterator != dpt_.cend(); iterator++) {
    if (min_redo_lsn > iterator->second) {
      min_redo_lsn = iterator->second;
    }
  }
  size_t all_log_record_size = flushed_lsn_ - min_redo_lsn;
  // std::cout << "all_log_record_size" << all_log_record_size << std::endl;
  char* log_data = new char[all_log_record_size];
  disk_.ReadLog(min_redo_lsn, all_log_record_size, log_data);
  size_t log_data_offset = 0;
  while (min_redo_lsn + log_data_offset < flushed_lsn_ + 1) {
    // std::cout << "offset " << log_data_offset << " ";
    auto log_record = LogRecord::DeserializeFrom(log_data + log_data_offset);
    log_record->SetLSN(min_redo_lsn + log_data_offset);
    // 本实验在开始检查点日志和结束检查点日志之间不存在其他日志
    switch (log_record->GetType()) {
      case LogType::INSERT:
      case LogType::DELETE:
      case LogType::NEW_PAGE:
        log_record->Redo(*buffer_pool_, *catalog_, *this, min_redo_lsn + log_data_offset);
        att_[log_record->GetXid()] = min_redo_lsn + log_data_offset;
        break;
      case LogType::BEGIN:
        // std::cout << "添加新事务" << log_record->GetXid() << std::endl;
        if (log_record->GetXid() != DDL_XID) {
          att_[log_record->GetXid()] = min_redo_lsn + log_data_offset;
        }
        break;
      case LogType::COMMIT:
        // std::cout << "事务提交：" << log_record->GetXid() << std::endl;
        att_.erase(log_record->GetXid());
        break;
      case LogType::ROLLBACK:
        // std::cout << "事务xid：" << log_record->GetXid() << " " 
        // << " " << att_.count(log_record->GetXid()) << std::endl;
        Rollback(log_record->GetXid());
        att_.erase(log_record->GetXid());
        break;
      case LogType::BEGIN_CHECKPOINT:
        // std::cout << "begin checkpoint log" <<std::endl;
      case LogType::END_CHECKPOINT:
      // std::cout << "end checkpoint log" <<std::endl;
        break;
      default:
        throw DbException("Unknown log type");
    }    
    log_data_offset += log_record->GetSize();
    log_buffer_.push_back(std::move(log_record));
  }
  // std::cout << "redo完后此时attsize为" << att_.size() << std::endl;
  delete[] log_data;
  
}

void LogManager::Undo() {
  // 根据活跃事务表，将所有活跃事务回滚
  // LAB 2 BEGIN
  // std::cout << "调用undo flush_lsn:" << flushed_lsn_ << std::endl;
  auto iterator = att_.begin();
  for (; iterator != att_.end();) {
    Rollback(iterator->first);
    iterator = att_.erase(iterator);    
  }
  // std::cout << "undo 结束，attsize为:" << att_.size() <<std::endl;
  
}

}  // namespace huadb
