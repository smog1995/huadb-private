#include "table/table_scan.h"

#include <memory>

#include "common/constants.h"
#include "common/typedefs.h"
#include "iostream"
#include "table/table_page.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {
  current_table_page_ =
      std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), table_->GetFirstPageId()));
  current_table_page_id_ = table_->GetFirstPageId();
}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // 每次调用读取一条记录
  // 读取时更新 rid_ 变量，避免重复读取
  // 扫描结束时，返回空指针
  // LAB 1 BEGIN
  // std::cout<< "scan";
  // std::cout <<"该scan语句的事务和sqlid为:"  << xid << " " << cid << std::endl;
  if (current_table_page_->GetRecordCount() == 0 || rid_.slot_id_ >= current_table_page_->GetRecordCount()) {
    std::cout << "尝试获取下一页：" << std::endl;
    if (current_table_page_->GetNextPageId() != NULL_PAGE_ID) {
      std::cout << "下一页" << std::endl;
      rid_.page_id_ = current_table_page_->GetNextPageId();
      rid_.slot_id_ = 0;
      current_table_page_ =
          std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_));
          std::cout << " pageId" << current_table_page_->GetNextPageId() << std::endl;
    } else {  //  读取结束
    // std::cout << "读取结束" << std::endl;
      return nullptr;
    }
  }
  auto current_record = current_table_page_->GetRecord(rid_.slot_id_, table_->GetColumnList());
  // std::cout << current_record->GetXmin() << "XMIN" << std::endl;
  bool is_deleted = current_record->IsDeleted();
  xid_t record_xid = current_record->GetXmin();
  cid_t record_cid = current_record->GetCid();
  while (is_deleted || (record_xid == xid && record_cid == cid && cid != NULL_CID)) {
    if (is_deleted) {
      // std::cout << "是被删除的记录" << " ";
    }
    if (record_xid == xid && record_cid == cid && cid != NULL_CID) {
      std::cout << "万圣节问题";
      break;
    }
    // std::cout << std::endl;
    rid_.slot_id_++;
    if (rid_.slot_id_ >= current_table_page_->GetRecordCount()) {
      if (current_table_page_->GetNextPageId() != NULL_PAGE_ID) {
        rid_.page_id_ = current_table_page_->GetNextPageId();
        current_table_page_ =
            std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_));
        rid_.slot_id_ = 0;
      } else {  //  读取结束
        return nullptr;
        // std::cout << "读取结束2" << std::endl;
      }
    }
    current_record = current_table_page_->GetRecord(rid_.slot_id_, table_->GetColumnList());
    is_deleted = current_record->IsDeleted();
    record_xid = current_record->GetXmin();
    record_cid = current_record->GetCid();
  }
  if (is_deleted || (record_xid == xid && record_cid == cid && cid != NULL_CID)) {
    return nullptr;
  }
  Record record(*current_record);
  record.SetRid(rid_);
  rid_.slot_id_++;
  return std::make_shared<Record>(record);
}

}  // namespace huadb
