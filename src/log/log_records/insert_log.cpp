#include "log/log_records/insert_log.h"
#include <memory>
#include "common/constants.h"
#include "common/typedefs.h"
#include "table/record.h"
#include "table/table.h"
#include "iostream"
namespace huadb {

InsertLog::InsertLog(xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t page_offset,
                     db_size_t record_size, char *record)
    : LogRecord(LogType::INSERT, xid, prev_lsn),
      oid_(oid),
      page_id_(page_id),
      slot_id_(slot_id),
      page_offset_(page_offset),
      record_size_(record_size),
      record_(record) {
  size_ +=
      sizeof(oid_) + sizeof(page_id_) + sizeof(slot_id_) + sizeof(page_offset_) + sizeof(record_size_) + record_size_;
}

InsertLog::~InsertLog() { delete[] record_; }

size_t InsertLog::SerializeTo(char *data) const {
  size_t offset = LogRecord::SerializeTo(data);
  memcpy(data + offset, &oid_, sizeof(oid_));
  offset += sizeof(oid_);
  memcpy(data + offset, &page_id_, sizeof(page_id_));
  offset += sizeof(page_id_);
  memcpy(data + offset, &slot_id_, sizeof(slot_id_));
  offset += sizeof(slot_id_);
  memcpy(data + offset, &page_offset_, sizeof(page_offset_));
  offset += sizeof(page_offset_);
  memcpy(data + offset, &record_size_, sizeof(record_size_));
  offset += sizeof(record_size_);
  memcpy(data + offset, record_, record_size_);
  offset += record_size_;
  assert(offset == size_);
  return offset;
}

std::shared_ptr<InsertLog> InsertLog::DeserializeFrom(const char *data) {
  xid_t xid;
  lsn_t prev_lsn;
  oid_t oid;
  pageid_t page_id;
  slotid_t slot_id;
  db_size_t page_offset, record_size;
  char *record;
  size_t offset = 0;
  memcpy(&xid, data + offset, sizeof(xid));
  offset += sizeof(xid);
  memcpy(&prev_lsn, data + offset, sizeof(prev_lsn));
  offset += sizeof(prev_lsn);
  memcpy(&oid, data + offset, sizeof(oid));
  offset += sizeof(oid);
  memcpy(&page_id, data + offset, sizeof(page_id));
  offset += sizeof(page_id);
  memcpy(&slot_id, data + offset, sizeof(slot_id));
  offset += sizeof(slot_id);
  memcpy(&page_offset, data + offset, sizeof(page_offset));
  offset += sizeof(page_offset);
  memcpy(&record_size, data + offset, sizeof(record_size));
  offset += sizeof(record_size);
  record = new char[record_size];
  memcpy(record, data + offset, record_size);
  offset += record_size;
  return std::make_shared<InsertLog>(xid, prev_lsn, oid, page_id, slot_id, page_offset, record_size, record);
}

void InsertLog::Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn,
                     lsn_t undo_next_lsn) {
  // 将插入的记录删除
  // LAB 2 BEGIN
  auto table = catalog.GetTable(oid_);
  std::cout <<"page_id" << page_id_ << " slotid " << slot_id_ <<std::endl;
  table->DeleteRecord({page_id_, slot_id_}, xid_, false);  //  这里暂时没设置写日志
  

}

void InsertLog::Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t lsn) {
  // 如果 oid_ 不存在，表示该表已经被删除，无需 redo
  // LAB 2 BEGIN
  if (oid_ == INVALID_OID) {
    return;
  }
  std::cout << "重做插入" << std::endl;
  auto table = catalog.GetTable(oid_);
  std::shared_ptr<Record> record;
  record.DeserializeFrom(record_, table->GetColumnList());
  table->InsertRecord(record, xid_, table->GetColumnList(), false);
}

oid_t InsertLog::GetOid() const { return oid_; }

pageid_t InsertLog::GetPageId() const { return page_id_; }

}  // namespace huadb
