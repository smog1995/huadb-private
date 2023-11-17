#include "storage/lru_buffer_strategy.h"

#include "fort.h"

namespace huadb {

void LRUBufferStrategy::Access(size_t frame_no) {
  // 缓存页面访问
  // LAB 1 BEGIN
  Node *node = nullptr;
  if (map_.count(frame_no) == 0) {
    node = new Node(frame_no);
    map_[frame_no] = node;
    double_list_.AddLast(node);
  }
  //  否则已在队列中
  node = map_[frame_no];
  node->accesses_++;
  //  按访问次数排序
  double_list_.RemoveX(node);
  double_list_.InsertX(node);
};

size_t LRUBufferStrategy::Evict() {
  // 缓存页面淘汰，返回淘汰的页面在 buffer pool 中的下标
  // LAB 1 BEGIN
  if (double_list_.Size() == 0) {
    return 0;
  }
  size_t victim = double_list_.Dequeue();
  map_.erase(victim);
  return victim;
}
LRUBufferStrategy::DoubleList::DoubleList() {
  head_ = new Node(0);
  tail_ = new Node(0);
  head_->next_ = tail_;
  tail_->pre_ = head_;
  size_ = 0;
}
void LRUBufferStrategy::DoubleList::AddLast(Node *x) {
  x->pre_ = tail_->pre_;
  x->next_ = tail_;
  tail_->pre_->next_ = x;
  tail_->pre_ = x;
  size_++;
}
void LRUBufferStrategy::DoubleList::InsertX(Node *x) {
  Node *p = head_->next_;
  while (p != tail_) {
    if (x->accesses_ < p->accesses_) {
      x->pre_ = p->pre_;
      x->next_ = p;
      p->pre_->next_ = x;
      p->pre_ = x;
      size_++;
      return;
    }
    p = p->next_;
  }
  // 如果到了队尾还没插入，说明时间戳为最大值，直接插入队尾
  AddLast(x);
}
void LRUBufferStrategy::DoubleList::RemoveX(Node *x) {
  x->pre_->next_ = x->next_;
  x->next_->pre_ = x->pre_;
  size_--;
}
auto LRUBufferStrategy::DoubleList::Dequeue() -> size_t {
  if (head_->next_ == tail_) {
    return 0;
  }
  Node *p = head_->next_;
  RemoveX(p);
  size_t frame_id = p->frame_id_;
  delete p;
  return frame_id;
}
LRUBufferStrategy::DoubleList::~DoubleList() {
  Node *p = head_;
  while (p != nullptr) {
    Node *next = p->next_;
    delete p;
    p = next;
  }
}
auto LRUBufferStrategy::DoubleList::Size() const -> size_t { return size_; }
}  // namespace huadb
