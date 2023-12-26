#include "table/table_scan.h"

#include <memory>
#include "common/constants.h"
#include "common/typedefs.h"
#include "iostream"
#include "table/table_page.h"
#include "transaction/transaction_manager.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {
  current_table_page_ =
      std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), table_->GetFirstPageId()));
  current_table_page_id_ = table_->GetFirstPageId();
}
// bool TableScan::MvccShowOrNot(Record* record, xid_t xid, IsolationLevel isolation_level,
//                               const std::unordered_set<xid_t> &active_xids) {

  
//   xid_t record_xmax = record->GetXmax();
//   xid_t record_xmin = record->GetXmin();
//   // 删除操作已提交
//   std::cout << "record_max:" << record_xmax << " xmin: " << record_xmin << " xid:" 
//   << xid << " level" << static_cast<int>(isolation_level) << std::endl;
//   if (record_xmax != NULL_XID 
//       && (active_xids.find(record_xmax) == active_xids.end() || record_xmax == xid)) {
//     std::cout << "该元组已删除" << std::endl;
//     return false;
//   }
//   if (record_xmin != xid
//       && active_xids.find(record_xmin) != active_xids.end()) {
//     std::cout << "该元组的插入还未提交" << std::endl;
//     return false;
//   }
//   std::cout << "可见" << std::endl;
//   return true;

// }
bool TableScan::MvccShowOrNot(Record* record, xid_t xid, IsolationLevel isolation_level,
                              const std::unordered_set<xid_t> &active_xids) {

  
  xid_t record_xmax = record->GetXmax();
  xid_t record_xmin = record->GetXmin();
  // 删除操作已提交
  std::cout << "record_max:" << record_xmax << " xmin: " << record_xmin << " xid:" 
  << xid << " level" << static_cast<int>(isolation_level) << std::endl;
  if (record_xmax <= xid && (active_xids.find(record_xmax) == active_xids.end() || record_xmax == xid)) {
    std::cout << "该元组已删除1" << std::endl;
    return false;
  }
  // 读已提交隔离级别，比当前事务新的事务已经提交该删除操作
  
  // if (record_xmax > xid && record_xmax != NULL_XID && isolation_level == IsolationLevel::READ_COMMITTED
  //     && active_xids.find(record_xmax) == active_xids.end()) {
  // if (record_xmax > xid && record_xmax != NULL_XID 
  //     && active_xids.find(record_xmax) == active_xids.end()) {
  //   std::cout << "当前读，该元组已删除" << std::endl;
  //   return false;
  // }
  // 如果是当前事务之后的新事务，隔离级别不是读已提交则不可见
    if (record_xmin > xid && isolation_level != IsolationLevel::READ_COMMITTED) {
    std::cout << "不为读未提交或读已提交，读取不到当前事务之后开始的新事务" << std::endl;
    return false;
  }
  // 如果是当前事务之后的新事务，隔离级别为读已提交，但未提交，则不可见
  if (record_xmin > xid && isolation_level == IsolationLevel::READ_COMMITTED 
      && active_xids.find(record_xmin) != active_xids.end()) {
        std::cout << "读已提交，可以读取到当前事务之后开始的新事务，但该新事务未提交" << std::endl;
    return false;
  }
  // 虽然为老事务但未提交
  if (record_xmin < xid && active_xids.find(record_xmin) != active_xids.end()) {
    std::cout << "虽然为老事务但未提交" << std::endl;
    return false;
  }
  // // 该元组插入操作并未提交
  // if (record_xmin != xid && active_xids.find(record_xmin) != active_xids.end()) {
  //   std::cout << "该元组插入未提交" << std::endl;
  //   return false;
  // }
  
  
  std::cout << "可见" << std::endl;
  return true;

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
    // std::cout << "尝试获取下一页：" << std::endl;
    if (current_table_page_->GetNextPageId() != NULL_PAGE_ID) {
      // std::cout << "下一页" << std::endl;
      rid_.page_id_ = current_table_page_->GetNextPageId();
      rid_.slot_id_ = 0;
      current_table_page_ =
          std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_));
          // std::cout << " pageId" << current_table_page_->GetNextPageId() << std::endl;
    } else {  //  读取结束
    std::cout << "读取结束" << std::endl;
      return nullptr;
    }
  }
  auto current_record = current_table_page_->GetRecord(rid_.slot_id_, table_->GetColumnList());
  bool is_show = !current_record->IsDeleted();
  if (xid != NULL_XID) {
    is_show = MvccShowOrNot(current_record.get(), xid, isolation_level, active_xids);
  } 
  xid_t record_xid = current_record->GetXmin();
  cid_t record_cid = current_record->GetCid();
  while (!is_show || (record_xid == xid && record_cid == cid && cid != NULL_CID)) {
    std::cout << "循环" << " ";
    if (record_xid == xid && record_cid == cid && cid != NULL_CID) {
      std::cout << "当前xid,cid:" << xid << " " <<cid << std::endl;
      std::cout << "该记录xid,cid:" << record_xid << " " << record_cid <<std::endl;
      std::cout << "万圣节问题";
    }
    rid_.slot_id_++;
    if (rid_.slot_id_ >= current_table_page_->GetRecordCount()) {
      if (current_table_page_->GetNextPageId() != NULL_PAGE_ID) {
        rid_.page_id_ = current_table_page_->GetNextPageId();
        current_table_page_ =
            std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_));
        rid_.slot_id_ = 0;
      } else {  //  读取结束
        std::cout << "读取结束2" << std::endl;
        return nullptr;
      }
    }
    current_record = current_table_page_->GetRecord(rid_.slot_id_, table_->GetColumnList());
    is_show = !current_record->IsDeleted();
    if (xid != NULL_XID) {
      is_show = MvccShowOrNot(current_record.get(), xid, isolation_level, active_xids);
    }
    record_xid = current_record->GetXmin();
    record_cid = current_record->GetCid();
  }
  if (!is_show || (record_xid == xid && record_cid == cid && cid != NULL_CID)) {
    std::cout << "读取结束2" << std::endl;
    return nullptr;
  }
  Record record(*current_record);
  record.SetRid(rid_);
  rid_.slot_id_++;
  return std::make_shared<Record>(record);
}

}  // namespace huadb
