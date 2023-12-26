#include "executors/lock_rows_executor.h"
#include "iostream"
namespace huadb {

LockRowsExecutor::LockRowsExecutor(ExecutorContext &context, std::shared_ptr<const LockRowsOperator> plan,
                                   std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void LockRowsExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> LockRowsExecutor::Next() {
  auto record = children_[0]->Next();
  if (record == nullptr) {
    return nullptr;
  }
  std::cout << "LowRowsExecutor" << std::endl;
  // 根据 plan_ 的 lock type 获取正确的锁，加锁失败时抛出异常
  // LAB 3 BEGIN
  bool lock_result = false;
  switch (plan_->GetLockType()) {
    case SelectLockType::NOLOCK :
      lock_result = true;
      break;
    case SelectLockType::SHARE :
      if (!context_.GetLockManager().LockRow(context_.GetXid(), LockType::S, plan_->GetOid(), record->GetRid())) {
        throw DbException("select for update 或share 上锁失败");
      } else {
        lock_result = true;
      }
      break;
    case SelectLockType::UPDATE :
      if (!context_.GetLockManager().LockRow(context_.GetXid(), LockType::X, plan_->GetOid(), record->GetRid())) {
        throw DbException("selectforupdate或share上锁失败");
      } else {
        lock_result = true;
      }
      break;
  }
  
  return record;
}

}  // namespace huadb
