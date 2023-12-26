#include "executors/insert_executor.h"
#include "binder/expression.h"
#include "common/exceptions.h"
#include "iostream"
namespace huadb {

InsertExecutor::InsertExecutor(ExecutorContext &context, std::shared_ptr<const InsertOperator> plan,
                               std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void InsertExecutor::Init() {
  std::cout << "插入executor" << std::endl;
  //  value executor引擎初始化
  children_[0]->Init();
  
  table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
  // insert操作对目标表加的是IX
  if (!context_.GetLockManager().LockTable(context_.GetXid(), LockType::IX, table_->GetOid())) {
    std::cout << "上表锁失败" << std::endl;
    throw DbException("插入执行器：上表锁失败");
  }
  column_list_ = context_.GetCatalog().GetTableColumnList(plan_->GetTableOid());
}

std::shared_ptr<Record> InsertExecutor::Next() {
  if (finished_) {
    return nullptr;
  }
  std::cout << "insert executor" ;
  uint32_t count = 0;
  while (auto record = children_[0]->Next()) {
    std::vector<Value> values(column_list_.Length());
    const auto &insert_columns = plan_->GetInsertColumns().GetColumns();
    for (size_t i = 0; i < insert_columns.size(); i++) {
      auto column_index = column_list_.GetColumnIndex(insert_columns[i].GetName());
      values[column_index] = record->GetValue(i);
    }
    auto table_record = std::make_shared<Record>(std::move(values));
    // 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    // 不需要对行加锁,只需上表锁
    auto rid = table_->InsertRecord(std::move(table_record), context_.GetXid(), context_.GetCid());
    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb
