#include "executors/delete_executor.h"
#include "iostream"
namespace huadb {

DeleteExecutor::DeleteExecutor(ExecutorContext &context, std::shared_ptr<const DeleteOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
  table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
}

void DeleteExecutor::Init() {
  children_[0]->Init();
  // std::cout << "删除executor" << std::endl;
  if (!context_.GetLockManager().LockTable(context_.GetXid(), LockType::IX, table_->GetOid())) {
    throw DbException("删除执行器：上表锁失败");
  }
}

std::shared_ptr<Record> DeleteExecutor::Next() {
  if (finished_) {
    return nullptr;
  }
  uint32_t count = 0;
  auto active_txn = context_.GetTransactionManager().GetActiveTransactions();
  while (auto record = children_[0]->Next()) {
    // 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    if (!context_.GetLockManager().LockRow(context_.GetXid(), LockType::X, table_->GetOid(), record->GetRid())) {
      throw DbException("上锁失败");
    }
    
    //  在删除时会进行当前读；可重复读、可串行化隔离级别下，如果删除该元组的事务已经提交了，那么当前读会导致该元组更新失败
    // if (record->GetXmax() != NULL_XID && active_txn.find(record->GetXmax()) == active_txn.end()) {
    //   throw DbException("修改了不存在的元组");
    // }
    table_->DeleteRecord(record->GetRid(), context_.GetXid());
    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb
