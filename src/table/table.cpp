#include "table/table.h"

#include <memory>

#include "common/constants.h"
#include "common/typedefs.h"
#include "iostream"
#include "table/table_page.h"
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
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }
  // std::cout << "insert" << std::endl;
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
  auto target_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, current_page_id_));
  if (record->GetSize() + RECORD_HEADER_SIZE > target_page->GetFreeSpaceSize()) {
    std::cout<<"add new Page" <<std::endl;
    target_page->SetNextPageId(++current_page_id_);
    target_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, current_page_id_));
    // lab2: 新添加页的操作记录到日志
    if (write_log) {
      lsn_t new_page_lsn = log_manager_.
                AppendNewPageLog(xid, oid_, current_page_id_ - 1, current_page_id_);  //  lab2 ，添加新page log
    }
    target_page->Init();
  }
  //  将记录插入对应page中
  slotid_t slot_id = target_page->InsertRecord(record, xid, cid);
  // std::cout<<"table SlotId"<< slot_id <<  " page:" <<  current_page_id_ << std::endl;
  //   InsertLog(xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t page_offset,
  //       db_size_t record_size, char *record);
  // lab2: 添加插入的日志记录
  //  appendInsertLog(事务id，对象id，当前页id，插入页的插槽id，页偏移，记录的数据大小，记录的地址)
  if (write_log) {
    char* record_data = new char[record->GetSize()];  //  因为通过delete[]删除，所以得分配到堆区
    record->SerializeTo(record_data);
    //  将删除记录写入日志
    lsn_t new_record_lsn = log_manager_.AppendInsertLog(xid, oid_, current_page_id_, slot_id, target_page->GetUpper(), record->GetSize(), record_data);
  }
  return {current_page_id_, slot_id};
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 TablePage 结构体操作记录页面
  // LAB 1 BEGIN
  auto target_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  target_page->DeleteRecord(rid.slot_id_, xid);
  if (write_log) {
    lsn_t delete_lsn = log_manager_.AppendDeleteLog(xid, oid_, rid.page_id_, rid.slot_id_);
  }
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
