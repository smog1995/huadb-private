#include "executors/seqscan_executor.h"
#include "iostream"
#include "transaction/transaction_manager.h"
namespace huadb {

SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)) {
      if (context_.GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        active_xids_ = context_.GetTransactionManager().GetSnapshot(context_.GetXid());
      } else if (context_.GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        active_xids_ = context_.GetTransactionManager().GetActiveTransactions();
      }
    }

void SeqScanExecutor::Init() {
  std::cout << "查询引擎初始化" << std::endl;
  
  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  LockType locktype = context_.IsModificationSql() ? LockType::IX : LockType::IS;
  std::cout << "查询引擎的事务发起者: " << context_.GetXid() << "sql语句id:" << context_.GetCid() 
  << "是否为修改sql语句："<< context_.IsModificationSql() <<" "<< static_cast<int>(locktype) << std::endl;
  if (!context_.GetLockManager().LockTable(context_.GetXid(), locktype, table->GetOid())) {
    throw DbException("查询执行器：上表锁失败");
  }
  scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
}

std::shared_ptr<Record> SeqScanExecutor::Next() {
  std::unordered_set<xid_t> active_xids = active_xids_;
  
  // std::cout << context_.GetXid() << " " << active_xids.size() << std::endl;
  // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
  // LAB 3 BEGIN
  // std::cout << "next" << std::endl;
  return scan_->GetNextRecord(context_.GetXid(), context_.GetIsolationLevel(), context_.GetCid(), active_xids);
}

}  // namespace huadb
