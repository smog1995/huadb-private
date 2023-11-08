#include "table/table.h"

#include "table/table_page.h"
#include "iostream"
namespace huadb {

Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
             bool new_table)
    : buffer_pool_(buffer_pool),
      log_manager_(log_manager),
      oid_(oid),
      db_oid_(db_oid),
      column_list_(std::move(column_list)) {
  std::shared_ptr<Page> page;
  if (new_table) {
    auto table_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, 0));
    table_page->Init();
    if (oid >= PRESERVED_OID) {
      lsn_t lsn = log_manager_.AppendNewPageLog(DDL_XID, oid_, NULL_PAGE_ID, 0);
      table_page->SetPageLSN(lsn);
    }
  }
  first_page_id_ = 0;
  current_page_id_ = 0;
}

Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
  
  if (record->GetSize() > MAX_RECORD_SIZE) {
    printf("异常");
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }
  printf("table的插入\n");
  // 当 write_log 参数为 true 时开启写日志功能
  // 在插入记录时增加写 InsertLog 过程
  // 在创建新的页面时增加写 NewPageLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 buffer_pool_ 获取页面
  // 使用 TablePage 类操作记录页面
  // 遍历表的页面，判断页面是否有足够的空间插入记录，如果没有则通过 buffer_pool_ 创建新页面
  // 创建新页面时需设置当前页面的 next_page_id，并将新页面初始化
  // 找到空间足够的页面后，通过 TablePage 插入记录
  // LAB 1 BEGIN
  auto next_page_id = first_page_id_;
  auto target_page = std::make_unique<TablePage>(buffer_pool_.GetPage(GetDbOid(), GetOid(), first_page_id_));
  std::cout << "1111" << std::endl;
  while (target_page->GetFreeSpaceSize() == 0 && next_page_id != NULL_PAGE_ID) {
    printf("页没空位\n");
    next_page_id = target_page->GetNextPageId();
    if (next_page_id != NULL_PAGE_ID) {
      target_page = std::make_unique<TablePage>(buffer_pool_.GetPage(GetDbOid(), GetOid(), next_page_id));
    }
  }
  // std::cout << "ea1111" << std::endl;
  // 如果找不到空位，需要创建新的page
  if (target_page->GetFreeSpaceSize() == 0) {
    printf("创建新页\n");
    target_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, ++current_page_id_));
    target_page->Init();
  }
  std::cout << "ea1111" << std::endl;
  target_page->InsertRecord(record, xid, cid);
  return {0, 0};
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 TablePage 结构体操作记录页面
  // LAB 1 BEGIN
}

Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
  DeleteRecord(rid, xid, write_log);
  return InsertRecord(record, xid, cid, write_log);
}

pageid_t Table::GetFirstPageId() const { return first_page_id_; }

oid_t Table::GetOid() const { return oid_; }

oid_t Table::GetDbOid() const { return db_oid_; }

const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
