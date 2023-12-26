#include "table/table_page.h"

#include <memory>

#include "common/constants.h"
#include "common/typedefs.h"
#include "cstring"
#include "iostream"
#include "table/record_header.h"
namespace huadb {

TablePage::TablePage(std::shared_ptr<Page> page) : page_(page) {
  page_data_ = page->GetData();
  db_size_t offset = 0;
  page_lsn_ = reinterpret_cast<lsn_t *>(page_data_);
  offset += sizeof(lsn_t);
  next_page_id_ = reinterpret_cast<pageid_t *>(page_data_ + offset);
  offset += sizeof(pageid_t);
  lower_ = reinterpret_cast<db_size_t *>(page_data_ + offset);
  offset += sizeof(db_size_t);
  upper_ = reinterpret_cast<db_size_t *>(page_data_ + offset);
  offset += sizeof(db_size_t);
  assert(offset == PAGE_HEADER_SIZE);
  slots_ = reinterpret_cast<Slot *>(page_data_ + PAGE_HEADER_SIZE);
}

void TablePage::Init() {
  *page_lsn_ = 0;
  *next_page_id_ = NULL_PAGE_ID;
  *lower_ = PAGE_HEADER_SIZE;
  *upper_ = DB_PAGE_SIZE;
  page_->SetDirty();
}

slotid_t TablePage::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid) {
  // 在记录头添加事务信息（xid 和 cid）
  // LAB 3 BEGIN

  // 维护 lower 和 upper 指针
  // 设置 slots 数组
  // 将 record 写入 page data
  // 将 page 标记为 dirty
  // LAB 1 BEGIN
  *upper_ -= record->GetSize();
  db_size_t record_size = record->GetSize();
  record->SerializeTo(page_data_ + *upper_);  //  写入record
  char* recordheader_data = new char[RECORD_HEADER_SIZE];
  record->SetXmin(xid);
  record->SetCid(cid);
  record->SerializeHeaderTo(recordheader_data);
  *upper_ -= RECORD_HEADER_SIZE;
  memcpy(page_data_ + *upper_, recordheader_data, RECORD_HEADER_SIZE);
  delete[] recordheader_data;
  record_size += RECORD_HEADER_SIZE;
  // 写入slot，slot包含记录偏移量和大小
  memcpy(page_data_ + *lower_, upper_, sizeof(db_size_t));            // 将记录偏移量写入
  memcpy(page_data_ + *lower_ + 2, &record_size, sizeof(db_size_t));  //  将记录大小写入
  *lower_ += 4;
  //  slots(当前slot的位置[lower] - slots数组地址) / 4 即为当前slot下标
  slotid_t slot_id = ((*lower_ - sizeof(page_lsn_) - sizeof(pageid_t) - sizeof(db_size_t) - sizeof(db_size_t)) / sizeof(Slot)) - 1;
  page_->SetDirty();
  return slot_id;
}

void TablePage::DeleteRecord(slotid_t slot_id, xid_t xid) {
  // 更改实验1的实现，改为通过 xid 标记删除
  // LAB 3 BEGIN
  // 将 slot_id 对应的 record 标记为删除
  // 将 page 标记为 dirty
  // LAB 1 BEGIN
  Slot slot = slots_[slot_id];
  // std::cout << slot.offset_ << " " << slot.size_ <<std::endl;
  bool deleted = true;
  // recordHeader成员顺序：deleted_,xmin_,xmax_,cid_ ,需修改deleted_和xmax_
  size_t offset = slot.offset_;
  memcpy(page_data_ + offset, &deleted, sizeof(bool));
  offset += sizeof(bool) + sizeof(xid_t);
  memcpy(page_data_ + offset, &xid, sizeof(xid_t));
  offset += sizeof(xid_t) + sizeof(cid_t);
  memcpy(page_data_ + offset, &deleted, sizeof(bool));
  page_->SetDirty();
}

std::unique_ptr<Record> TablePage::GetRecord(slotid_t slot_id, const ColumnList &column_list) {
  // 根据 slot_id 获取 record
  // LAB 1 BEGIN
  Slot slot = slots_[slot_id];
  Record record;
  record.DeserializeFrom(page_data_ + slot.offset_ + RECORD_HEADER_SIZE, column_list);
  std::unique_ptr<Record> rec_ptr;
  rec_ptr = std::make_unique<Record>(record);
  rec_ptr->DeserializeHeaderFrom(page_data_ + slot.offset_);
  // std::cout << rec_ptr->GetXmin() <<std::endl;
  return rec_ptr;
}

void TablePage::UndoDeleteRecord(slotid_t slot_id) {
  // 修改 undo delete 的逻辑
  // LAB 3 BEGIN
  // 清除记录的删除标记
  // LAB 2 BEGIN
  Slot slot = slots_[slot_id];
  size_t offset = slot.offset_;
  // recordHeader成员顺序：deleted_,xmin_,xmax_,cid_ ,需修改deleted_为false和xmax_改为最大事务大小
  bool deleted = false;
  memcpy(page_data_ + offset, &deleted, sizeof(bool));
  offset += sizeof(bool) + sizeof(xid_t);
  memcpy(page_data_ + offset, &NULL_XID, sizeof(xid_t));
  offset += sizeof(xid_t) + sizeof(cid_t);
  memcpy(page_data_ + offset, &deleted, sizeof(bool));
  page_->SetDirty();
}

void TablePage::RedoInsertRecord(slotid_t slot_id, char *raw_record, db_size_t page_offset, db_size_t record_size) {
  // 将 raw_record 写入 page data
  // 注意维护 lower 和 upper 指针，以及 slots 数组
  // 将页面设为 dirty
  // LAB 2 BEGIN
  memcpy(page_data_ + page_offset, raw_record, record_size); 
  // 需要满足幂等性，只有此刻表确实没成功插入时，upper需要移动（其他情况则是upper不位于此处）
  if (*upper_ == page_offset + record_size) {
    *upper_ -= record_size;
    *lower_ += 4; 
  }
  //  写入slot，slot包含记录偏移量和大小
  memcpy(&slots_[slot_id], &page_offset, sizeof(db_size_t));            // 将记录偏移量写入
  memcpy(&slots_[slot_id] + sizeof(db_size_t), &record_size, sizeof(db_size_t));  //  将记录大小写入
  page_->SetDirty();
}

db_size_t TablePage::GetRecordCount() const { return (*lower_ - PAGE_HEADER_SIZE) / sizeof(Slot); }

lsn_t TablePage::GetPageLSN() const { return *page_lsn_; }

pageid_t TablePage::GetNextPageId() const { return *next_page_id_; }

db_size_t TablePage::GetLower() const { return *lower_; }

db_size_t TablePage::GetUpper() const { return *upper_; }

db_size_t TablePage::GetFreeSpaceSize() {
  if (*upper_ < *lower_ + sizeof(Slot)) {
    return 0;
  } else {
    return *upper_ - *lower_ - sizeof(Slot);
  }
}

void TablePage::SetNextPageId(pageid_t page_id) {
  *next_page_id_ = page_id;
  page_->SetDirty();
}

void TablePage::SetPageLSN(lsn_t page_lsn) {
  *page_lsn_ = page_lsn;
  page_->SetDirty();
}

}  // namespace huadb
