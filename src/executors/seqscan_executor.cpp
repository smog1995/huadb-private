#include "executors/seqscan_executor.h"
#include "iostream"
#include "transaction/transaction_manager.h"
namespace huadb {

SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)), 
    active_xids_(context.GetTransactionManager().GetActiveTransactions()){}

void SeqScanExecutor::Init() {
  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
  if (context_.GetIsolationLevel() == IsolationLevel::READ_COMMITTED) { // 读已提交，可以看见最新提交的事务更改
    active_xids_.clear();
    active_xids_ = context_.GetTransactionManager().GetActiveTransactions();
  }
}

std::shared_ptr<Record> SeqScanExecutor::Next() {
  std::unordered_set<xid_t> active_xids;
  // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
  // LAB 3 BEGIN
  // std::cout << "next" << std::endl;
  return scan_->GetNextRecord(context_.GetXid(), context_.GetIsolationLevel(), context_.GetCid(), active_xids);
}

}  // namespace huadb
