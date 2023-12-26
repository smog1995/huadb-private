#pragma once
#include <mutex>
#include <memory>
#include <list>
#include <unordered_map>
#include "common/constants.h"
#include "common/typedefs.h"


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
      LockRequest(xid_t xid, LockType lock_type, oid_t oid) //  表锁请求的构造
          : xid_(xid), lock_type_(lock_type), oid_(oid) {}
      LockRequest(xid_t txn_id, LockType lock_type, oid_t oid, Rid rid) //  行锁请求的构造
          : xid_(txn_id), lock_type_(lock_type), oid_(oid), rid_(rid) {}

      //  记录执行上锁的事务id
      xid_t xid_;
      //  锁类型
      LockType lock_type_;
      //  表的对象id，记录了一个行所属的表id或者一个表的表id
      oid_t oid_;
      // 行的rid
      Rid rid_;
    };
  class LockRequestQueue {
    public:
    //  锁请求队列
    std::list<std::unique_ptr<LockRequest>> request_queue_;
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
  bool upgrade_lock_[5][5] = {{true, true, true, false, false},
                             {false, true, false, true, true},
                             {false, false, true, true, true},
                             {false, true, true, true, true},
                             {false, false, false, false, true}};
  bool compatable_lock_[5][5] = {{true, true, true, true, false},
                                 {true, true, false, false, false},
                                 {true, false, true, false, false},
                                 {true, false, false, false, false},
                                 {false, false, false, false, false}};
  bool Compatible(LockType type_a, LockType type_b);
  

  //  rid的哈希函数实现
  struct RidHash {
    size_t operator()(const Rid& rid) const {
        // 一种简单的哈希函数示例，可以根据实际情况修改
        return std::hash<pageid_t>()(rid.page_id_) ^ std::hash<slotid_t>()(rid.slot_id_);
    }
  };
  
  // 实现锁的升级
  LockType Upgrade(LockType self, LockType other);

  DeadlockType deadlock_type_ = DeadlockType::NONE;

  std::mutex row_lock_map_latch_;
  std::mutex table_lock_map_latch_;

  std::unordered_map<oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  
  std::unordered_map<Rid, std::shared_ptr<LockRequestQueue>,RidHash> row_lock_map_;
};

}  // namespace huadb
