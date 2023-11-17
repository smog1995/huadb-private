#pragma once

#include <cstddef>

#include "storage/buffer_strategy.h"
#include "unordered_map"
namespace huadb {

class LRUBufferStrategy : public BufferStrategy {
 public:
  void Access(size_t frame_no) override;
  size_t Evict() override;
  struct Node {
    Node *pre_, *next_;
    size_t frame_id_;
    size_t accesses_;
    bool evictable_;
    explicit Node(size_t fid) : pre_(nullptr), next_(nullptr), frame_id_(fid), accesses_(0), evictable_(false) {}
  };
  class DoubleList {
   public:
    DoubleList();
    void AddLast(Node *x);
    void RemoveX(Node *x);
    void InsertX(Node *x);
    auto Dequeue() -> size_t;
    inline auto Size() const -> size_t;
    ~DoubleList();

   private:
    Node *head_, *tail_;
    size_t size_;
  };

 private:
  std::unordered_map<size_t, Node *> map_;
  DoubleList double_list_;
};

}  // namespace huadb
