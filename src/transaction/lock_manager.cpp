#include "transaction/lock_manager.h"
#include "iostream"

namespace huadb {

bool LockManager::LockTable(xid_t xid, LockType lock_type, oid_t oid) {
  // 对数据表加锁，成功加锁返回 true，如果数据表已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据表的锁，根据需要升级锁的类型
  // LAB 3 BEGIN
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_.insert(std::make_pair(oid, std::make_shared<LockRequestQueue>()));
  }
  auto lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
 
  /*
   * 锁升级的实现思路是，首先找到该事务xid想要上锁的表的锁请求队列，
   * 遍历一遍队列锁升级的类型是否与其他事务上的锁类型兼容，若兼容，再
   * 判断之前xid上过的锁能否直接升级，不能则再上新锁（锁类型为想要升级的类型）。
   */
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
    if ((*ele)->xid_ != xid && !Compatible((*ele)->lock_type_, lock_type)) {
      std::cout << "上锁不兼容" << static_cast<int>((*ele)->lock_type_) << " :" << static_cast<int>(lock_type)<< std::endl;
      std::cout << xid << "该锁xid" << (*ele)->xid_ << std::endl;
      return false;
    }
  }
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
    if ((*ele)->xid_ == xid) {
      if ((*ele)->lock_type_ == lock_type) {
        std::cout << "(原地升级锁,但是升级类型相同，无需升级)上表锁：" << oid << "锁类型" << static_cast<int>(lock_type) << std::endl;
        return true;
      }
      LockType upgrade_lock = Upgrade((*ele)->lock_type_, lock_type);
      if (upgrade_lock == (*ele)->lock_type_) { // 说明不能原地升级
        //只能加新锁
        break;
      } else { // 否则直接原地升级
        (*ele)->lock_type_ = upgrade_lock;
        std::cout << "(原地升级锁)上表锁：" << oid << "锁类型" << static_cast<int>(lock_type) << std::endl;
        return true;
      }
    }
  }
  auto lock_request = new LockRequest(xid, lock_type, oid);
  lock_request_queue->request_queue_.push_back(std::unique_ptr<LockRequest>(lock_request));
  std::cout << "上表锁：" << oid << "锁类型" << static_cast<int>(lock_type) << std::endl;
  return true;
}

bool LockManager::LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid) {
  // 对数据行加锁，成功加锁返回 true，如果数据行已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据行的锁，根据需要升级锁的类型
  // LAB 3 BEGIN
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.insert(std::make_pair(rid, std::make_shared<LockRequestQueue>()));
  }
  auto lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);

  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
    if ((*ele)->xid_ != xid && !Compatible((*ele)->lock_type_, lock_type)) {
      return false;
    }
  }
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
    if ((*ele)->xid_ == xid) {
      LockType upgrade_lock_ = Upgrade((*ele)->lock_type_, lock_type);
      if (upgrade_lock_ == (*ele)->lock_type_) { // 说明不能原地升级
        //只能加新锁
        break;
      } else { // 否则直接原地升级
        (*ele)->lock_type_ = upgrade_lock_;
        std::cout << "(原地升级）上行锁：" << rid.page_id_ << ", " << rid.slot_id_ << " 锁类型" << static_cast<int>(lock_type) << std::endl;
        return true;
      }
    }
  }
  auto lock_request = new LockRequest(xid, lock_type, oid);
  lock_request_queue->request_queue_.push_back(std::unique_ptr<LockRequest>(lock_request));
  std::cout << "上行锁：" << rid.page_id_ << ", " << rid.slot_id_ << " 锁类型" << static_cast<int>(lock_type) << std::endl;
  return true;
}

void LockManager::ReleaseLocks(xid_t xid) {
  // 释放事务 xid 持有的所有锁
  // LAB 3 BEGIN
  // 先解所有行锁
  for (auto &[rid, lock_request_queue] : row_lock_map_) {
    for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end();) {
      if ((*ele)->xid_ == xid) {
        std::cout << "释放行锁：" << rid.page_id_ << "," << rid.slot_id_<< " 锁类型" << static_cast<int>((*ele)->lock_type_) << std::endl;
        ele = lock_request_queue->request_queue_.erase(ele++);
      } else {
        ele++;
      }
    }
  }
  // 再解表锁
  for (auto &[oid, lock_request_queue] : table_lock_map_) {
    for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end();) {
      if ((*ele)->xid_ == xid) {
        std::cout << "释放表锁：" << oid << " 锁类型" << static_cast<int>((*ele)->lock_type_) << std::endl;
        ele = lock_request_queue->request_queue_.erase(ele++);
      } else {
        ele++;
      }
    }
  }
  
}

void LockManager::SetDeadLockType(DeadlockType deadlock_type) { deadlock_type_ = deadlock_type; }

bool LockManager::Compatible(LockType type_a, LockType type_b) {
  // 判断锁是否相容
  // LAB 3 BEGIN
  return compatable_lock_[static_cast<int>(type_a)][static_cast<int>(type_b)];
}

LockType LockManager::Upgrade(LockType self, LockType other) {
  // 升级锁类型
  // LAB 3 BEGIN
  if (upgrade_lock_[static_cast<int>(self)][static_cast<int>(other)]) {
    return other;
  }
  return self;
}

}  // namespace huadb
