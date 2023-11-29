#include "table/table_page.h"

#include <memory>

#include "common/typedefs.h"
#include "cstring"
#include "iostream"
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
  // std::cout << "tablepageinsert" << std::endl;
  // 在记录头添加事务信息（xid 和 cid）
  // LAB 3 BEGIN

  // 维护 lower 和 upper 指针
  // 设置 slots 数组
  // 将 record 写入 page data
  // 将 page 标记为 dirty
  // LAB 1 BEGIN
  page_->SetDirty();
  db_size_t record_size = record->SerializeTo(page_data_ + *upper_ - record->GetSize());  //  写入record
  *upper_ -= record->GetSize();
  //  写入slot，slot包含记录偏移量和大小
  memcpy(page_data_ + *lower_, upper_, sizeof(db_size_t));            // 将记录偏移量写入
  memcpy(page_data_ + *lower_ + 2, &record_size, sizeof(db_size_t));  //  将记录大小写入
  *lower_ += 4;                                                       //  slot大小
  // slots(当前slot的位置[lower] - slots数组地址) / 4 即为当前slot下标
  slotid_t slot_id = (*lower_ - sizeof(page_lsn_) + sizeof(next_page_id_) + sizeof(lower_) + sizeof(upper_)) / sizeof(Slot) - 1;
  return 0;
}

void TablePage::DeleteRecord(slotid_t slot_id, xid_t xid) {
  // 更改实验1的实现，改为通过 xid 标记删除
  // LAB 3 BEGIN

  // 将 slot_id 对应的 record 标记为删除
  // 将 page 标记为 dirty
  // LAB 1 BEGIN
  page_->SetDirty();
  Slot slot = slots_[slot_id];
  bool deleted = true;
  memcpy(page_data_ + slot.offset_, &deleted, sizeof(bool));
}

std::unique_ptr<Record> TablePage::GetRecord(slotid_t slot_id, const ColumnList &column_list) {
  // 根据 slot_id 获取 record
  // LAB 1 BEGIN
  Slot slot = slots_[slot_id];
  Record record;
  record.DeserializeFrom(page_data_ + slot.offset_, column_list);
  return std::make_unique<Record>(record);
}

void TablePage::UndoDeleteRecord(slotid_t slot_id) {
  // 修改 undo delete 的逻辑
  // LAB 3 BEGIN
  
  // 清除记录的删除标记
  // LAB 2 BEGIN
  page_->SetDirty();
  Slot slot = slots_[slot_id];
  bool deleted = false;
  memcpy(page_data_ + slot.offset_, &deleted, sizeof(bool));

}

void TablePage::RedoInsertRecord(slotid_t slot_id, char *raw_record, db_size_t page_offset, db_size_t record_size) {
  // 将 raw_record 写入 page data
  // 注意维护 lower 和 upper 指针，以及 slots 数组
  // 将页面设为 dirty
  // LAB 2 BEGIN
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
