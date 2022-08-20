#pragma once

#include <xenium/reclamation/detail/allocation_tracker.hpp>
#include <xenium/reclamation/detail/concurrent_ptr.hpp>
#include <xenium/reclamation/detail/deletable_object.hpp>
#include <xenium/reclamation/detail/guard_ptr.hpp>
#include <xenium/reclamation/detail/thread_block_list.hpp>

#include <xenium/acquire_guard.hpp>
#include <xenium/parameter.hpp>
#include <xenium/policy.hpp>

#include <memory>
#include <stdexcept>

namespace xenium::reclamation {

template <class IR> // number of retries and min jump distance here
class reclaimer_rtm {
  template <class T, class MarkedPtr>
  class guard_ptr;

  using InnerReclaimer = typename IR::template with_derive<guard_ptr>;

public:
  template <class T, std::size_t N = 0, class Deleter = std::default_delete<T>>
  class enable_concurrent_ptr;

  struct region_guard : private InnerReclaimer::region_guard {
    region_guard() noexcept;
    ~region_guard() noexcept;

    region_guard(const region_guard&) = delete;
    region_guard(region_guard&&) = delete;
    region_guard& operator=(const region_guard&) = delete;
    region_guard& operator=(region_guard&&) = delete;
  };

  template <class T, std::size_t N = T::number_of_mark_bits>
  using concurrent_ptr = detail::concurrent_ptr<T, N, guard_ptr>;

  ALLOCATION_TRACKER;

private:
  class thread_data;

  inline static InnerReclaimer inner_reclaimer;
  inline static thread_local thread_data local_thread_data;

  ALLOCATION_TRACKING_FUNCTIONS;
};

template <class IR>
template <class T, std::size_t N, class Deleter>
class reclaimer_rtm<IR>::enable_concurrent_ptr
    : public InnerReclaimer::template enable_concurrent_ptr<T, N, Deleter>,
      private detail::tracked_object<reclaimer_rtm> {
protected:
  enable_concurrent_ptr() noexcept = default;
  enable_concurrent_ptr(const enable_concurrent_ptr&) noexcept = default;
  enable_concurrent_ptr(enable_concurrent_ptr&&) noexcept = default;
  enable_concurrent_ptr& operator=(const enable_concurrent_ptr&) noexcept = default;
  enable_concurrent_ptr& operator=(enable_concurrent_ptr&&) noexcept = default;
  ~enable_concurrent_ptr() noexcept override = default;
};

template <class Derived> // unique head for each derived type
struct double_linked_node {
  double_linked_node* prev_dl_node = nullptr;
  double_linked_node* next_dl_node = nullptr;

  double_linked_node() noexcept: prev_dl_node(head()), next_dl_node(head()->next_dl_node) { // links into global list
    prev_dl_node->next_dl_node = this;
    next_dl_node->prev_dl_node = this;
  }

  ~double_linked_node() noexcept { // unlinks from global list
    prev_dl_node->next_dl_node = next_dl_node;
    next_dl_node->prev_dl_node = prev_dl_node;
  }

  static double_linked_node* head() noexcept {
    static thread_local double_linked_node static_head(head_constructor_token{});
    return &static_head;
  }

private:
  struct head_constructor_token {};

  double_linked_node(head_constructor_token) noexcept: prev_dl_node(this), next_dl_node(this) {}
};

template <class IR>
template <class T, class MarkedPtr>
class reclaimer_rtm<IR>::guard_ptr
    : public InnerReclaimer::template concurrent_ptr<T>::guard_ptr,
      private double_linked_node<guard_ptr<T, MarkedPtr>> {
  using base = typename InnerReclaimer::template concurrent_ptr<T>::guard_ptr;
  using dl_node_base = double_linked_node<guard_ptr>;

protected:
  using friendly_base = typename base::friendly_base;
  using Deleter = typename base::Deleter;

public:
  guard_ptr() noexcept = default;

  // Guard a marked ptr.
  explicit guard_ptr(const MarkedPtr& p);
  guard_ptr(const guard_ptr& p);
  guard_ptr(guard_ptr&& p) noexcept;

  guard_ptr& operator=(const guard_ptr& p) noexcept;
  guard_ptr& operator=(guard_ptr&& p) noexcept;

  // Atomically take snapshot of p, and *if* it points to unreclaimed object, acquire shared ownership of it.
  void acquire(const concurrent_ptr<T>& p, std::memory_order order = std::memory_order_seq_cst);

  // Like acquire, but quit early if a snapshot != expected.
  bool acquire_if_equal(const concurrent_ptr<T>& p,
                        const MarkedPtr& expected,
                        std::memory_order order = std::memory_order_seq_cst);

  // Release ownership. Postcondition: get() == nullptr.
  void reset() noexcept;

  // Reset. Deleter d will be applied some time after all owners release their ownership.
  void reclaim(Deleter d = Deleter()) noexcept;

protected:
  friend friendly_base;
  void do_swap(guard_ptr& g) noexcept;

private:
  bool in_tx();
  bool in_tx_like(const guard_ptr& p);
  void maybe_commit_tx();

  void tx_acquire(const MarkedPtr& p) noexcept;
  void tx_reset() noexcept;
  void tx_reclaim(Deleter d) noexcept;

  void base_acquire();
  void base_acquire_all();

  bool acquired_in_tx = false;
};
} // namespace xenium::reclamation

#define RECLAIMER_RTM_IMPL
#include "impl/reclaimer_rtm.hpp"
#undef RECLAIMER_RTM_IMPL

