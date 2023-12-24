#include "transaction/lock_manager.h"
#include <memory>
#include <mutex>

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
  for (auto ele = lock_request_queue->request_queue_.begin(); ele != lock_request_queue->request_queue_.end(); ele++) {
    // 相同事务，可能为锁升级
    if ((*ele)->xid_ == xid) {
      LockType upgrade_lock_type = Upgrade((*ele)->lock_type_, lock_type);
      bool compatible = true;
      for (auto &iter :lock_request_queue->request_queue_) {
        if (!Compatible(upgrade_lock_type, iter->lock_type_)) {
          compatible = false;
          break;
        }
      }
      if (compatible) {
        (*ele)->lock_type_ = upgrade_lock_type; // 修改为新的锁类型
        return true;
      } else {
        return false;
      }
      //  不同事务，判断是否符合锁兼容
    } else if (!Compatible((*ele)->lock_type_, lock_type)) {
      return false;
    }
  }
  auto lock_request = std::make_unique(new LockRequest(xid, lock_type, oid));
  lock_request_queue->request_queue_.push_back(lock_request);
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
    // 相同事务，可能为锁升级
    if ((*ele)->xid_ == xid) {
      LockType upgrade_lock_type = Upgrade((*ele)->lock_type_, lock_type);
      bool compatible = true;
      for (auto &iter :lock_request_queue->request_queue_) {
        if (!Compatible(upgrade_lock_type, iter->lock_type_)) {
          compatible = false;
          break;
        }
      }
      if (compatible) {
        (*ele)->lock_type_ = upgrade_lock_type; // 修改为新的锁类型
        return true;
      } else {
        return false;
      }
      //  不同事务，判断是否符合锁兼容
    } else if (!Compatible((*ele)->lock_type_, lock_type)) {
      return false;
    }
  }
  auto lock_request = std::make_unique(new LockRequest(xid, lock_type, oid));
  lock_request_queue->request_queue_.push_back(lock_request);
  return true;
}

void LockManager::ReleaseLocks(xid_t xid) {
  // 释放事务 xid 持有的所有锁
  // LAB 3 BEGIN
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
  // 能升级则返回升级后的类型，否则返回原类型
  if (upgrade_lock_[static_cast<int>(self)][static_cast<int>(other)]) {
    return other;
  }
  return self;
}

}  // namespace huadb
