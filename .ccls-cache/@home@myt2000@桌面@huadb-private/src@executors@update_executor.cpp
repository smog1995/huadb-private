#include "executors/update_executor.h"
#include "iostream"
#include "transaction/transaction_manager.h"
namespace huadb {

UpdateExecutor::UpdateExecutor(ExecutorContext &context, std::shared_ptr<const UpdateOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void UpdateExecutor::Init() {
  children_[0]->Init();
  // std::cout << "修改引擎的事务发起者: " << context_.GetXid() << "sql语句id:" << context_.GetCid() << std::endl;
  table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
  // 不需要再上一次表锁，因为子执行器为删除引擎，已经上过一次表锁
  // if (context_.GetLockManager().LockTable(context_.GetXid(), LockType::IX, table_->GetOid())) {
  //   throw DbException("上表锁失败");
  // }
}

std::shared_ptr<Record> UpdateExecutor::Next() {
  if (finished_) {
    return nullptr;
  }
  auto active_txn = context_.GetTransactionManager().GetActiveTransactions();
  // std::cout << "update" << std::endl;
  uint32_t count = 0;
  while (auto record = children_[0]->Next()) {
    std::vector<Value> values;
    for (const auto &expr : plan_->update_exprs_) {
      values.push_back(expr->Evaluate(record));
    }
    auto new_record = std::make_shared<Record>(std::move(values));
    // 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    if (!context_.GetLockManager().LockRow(context_.GetXid(), LockType::X, table_->GetOid(), record->GetRid())) {
      throw DbException("上行锁失败");
    }
    
    //  在修改时会进行当前读；可重复读、可串行化隔离级别下，如果删除该元组的事务已经提交了，那么当前读会导致该元组更新失败
    if (record->GetXmax() != NULL_XID && active_txn.find(record->GetXmax()) == active_txn.end()) {
      // std::cout << "修改了不存在的元组" << std::endl;
      throw DbException("修改了不存在的元组");
    }
    auto rid = table_->UpdateRecord(record->GetRid(), context_.GetXid(), context_.GetCid(), new_record);
    // 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb
