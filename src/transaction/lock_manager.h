#pragma once

#include <mutex>
#include <memory>
#include <unordered_map>
#include "common/constants.h"
#include "common/typedefs.h"
#include "list"

namespace huadb {

enum class LockType {
  IS,   // 意向共享锁
  IX,   // 意向互斥锁
  S,    // 共享锁
  SIX,  // 共享意向互斥锁
  X,    // 互斥锁
};

enum class LockGranularity { TABLE, ROW };

// 高级功能：死锁预防/检测类型
enum class DeadlockType { NONE, WAIT_DIE, WOUND_WAIT, DETECTION };

class LockManager {
 public:
  class LockRequest {
    public:
      LockRequest(xid_t xid, LockType lock_type, oid_t oid) /** Table lock request */
          : xid_(xid), lock_type_(lock_type), oid_(oid) {}
      LockRequest(xid_t txn_id, LockType lock_type, oid_t oid, Rid rid) /** Row lock request */
          : xid_(txn_id), lock_type_(lock_type), oid_(oid), rid_(rid) {}

      /** Txn_id of the txn requesting the lock */
      xid_t xid_;
      /** Locking mode of the requested lock */
      LockType lock_type_;
      /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
      oid_t oid_;
      /** Rid of the row for a row lock; unused for table locks */
      Rid rid_;
    };
  class LockRequestQueue {
    public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::unique_ptr<LockRequest>> request_queue_;
    /** coordination */
    xid_t upgrading_ = DDL_XID;
    std::mutex latch_;
  };
  // 获取表级锁
  bool LockTable(xid_t xid, LockType lock_type, oid_t oid);
  // 获取行级锁
  bool LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid);

  // 释放事务申请的全部锁
  void ReleaseLocks(xid_t xid);

  void SetDeadLockType(DeadlockType deadlock_type);

 private:
  // 判断锁的相容性
  //  LockMode {IS IX S SIX X};
  bool upgrade_lock_[5][5] = {{true, true, false, false, true},
                             {false, false, false, false, false},
                             {true, true, true, true, true},
                             {false, true, false, true, true},
                             {false, true, false, false, false}};
  bool compatable_lock_[5][5] = {{true, true, true, true, false},
                                 {true, true, false, false, false},
                                 {true, false, true, false, false},
                                 {false, false, false, false, false},
                                 {false, false, false, false, false}};
  bool Compatible(LockType type_a, LockType type_b);
  
  
  // 实现锁的升级
  LockType Upgrade(LockType self, LockType other);

  DeadlockType deadlock_type_ = DeadlockType::NONE;

  std::mutex row_lock_map_latch_;
  std::mutex table_lock_map_latch_;

  std::unordered_map<oid_t, std::shared_ptr<LockRequestQueue> > table_lock_map_;
  
  std::unordered_map<Rid, std::shared_ptr<LockRequestQueue> > row_lock_map_;
};

}  // namespace huadb
