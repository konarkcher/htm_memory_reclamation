#ifndef RECLAIMER_RTM_IMPL
  #error "This is an impl file and must not be included directly!"
#endif

#include <xenium/aligned_object.hpp>
#include <xenium/detail/port.hpp>

#include <boost/fiber/detail/config.hpp>
#include <boost/fiber/detail/cpu_relax.hpp>
#include <boost/fiber/detail/rtm.hpp>
#include <boost/fiber/detail/spinlock_status.hpp>

#include <algorithm>
#include <new>
#include <vector>

#ifdef _MSC_VER
  #pragma warning(push)
  #pragma warning(disable : 4324) // structure was padded due to alignment specifier
#endif

namespace xenium::reclamation {

template <class IR>
reclaimer_rtm<IR>::region_guard::region_guard() noexcept {
  local_thread_data.begin_tx();
}

template <class IR>
reclaimer_rtm<IR>::region_guard::~region_guard() noexcept {
  if (!local_thread_data.in_tx()) {
    return;
  }
  local_thread_data.commit_tx();
}

template <class IR>
template <class T, class MarkedPtr>
reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::guard_ptr(const MarkedPtr& p) {
  if (!in_tx()) {
    base::operator=(base(p));
    return;
  }

  if (p.get() == nullptr) {
    return;
  }
  tx_acquire(p);
}

template <class IR>
template <class T, class MarkedPtr>
reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::guard_ptr(const guard_ptr& p) : guard_ptr(p.ptr) {}

template <class IR>
template <class T, class MarkedPtr>
reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::guard_ptr(guard_ptr&& p) noexcept {
  if (!in_tx_like(p)) {
    base::operator=(std::move(p));
    return;
  }

  tx_acquire(p.ptr);
  p.ptr.reset();
  p.acquired_in_tx = false;
}

template <class IR>
template <class T, class MarkedPtr>
auto reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::operator=(const guard_ptr& p) noexcept -> guard_ptr& {
  if (!in_tx()) {
    base::operator=(p);
    return *this;
  }

  if (&p == this) {
    return *this;
  }

  reset(); // can be an anchor
  if (p.ptr.get() == nullptr) { // TODO: WHY HE ASSUMES p NOT NULL???
    return *this;
  }

  tx_acquire(p.ptr);
  return *this;
}

template <class IR>
template <class T, class MarkedPtr>
auto reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::operator=(guard_ptr&& p) noexcept -> guard_ptr& {
  if (!in_tx_like(p)) {
    base::operator=(std::move(p));
    return *this;
  }

  if (&p == this) {
    return *this;
  }

  tx_acquire(p.ptr);
  p.ptr.reset();
  p.acquired_in_tx = false;
  return *this;
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::acquire(const concurrent_ptr<T>& p, std::memory_order order) {
  if (!in_tx()) {
    base::acquire(p, order);
    return *this;
  }

  auto p1 = p.load(order); // checking before reset is just an optimization
  if (p1 == this->ptr) { // empty or protected
    return;
  }
  reset(); // can be an anchor

  if (p1.get() == nullptr) {
    acquired_in_tx = false;
    return;
  }
  tx_acquire(p1);
}

template <class IR>
template <class T, class MarkedPtr>
bool reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::acquire_if_equal(const concurrent_ptr<T>& p,
                                                                  const MarkedPtr& expected,
                                                                  std::memory_order order) {
  if (!in_tx()) {
    return base::acquire_if_equal(p, expected, order);
  }
  reset(); // can be an anchor

  auto p1 = p.load(order);
  if (p1.get() == nullptr || p1 != expected) {
    return p1 == expected;
  }

  tx_acquire(p1);
  return true;
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::reset() noexcept {
  if (!acquired_in_tx) {
    base::reset();
    return;
  }
  tx_reset();
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::reclaim(Deleter d) noexcept {
  if (!acquired_in_tx) {
    base::reclaim(std::move(d));
    return;
  }
  tx_reclaim(std::move(d));
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::do_swap(guard_ptr& g) noexcept {
  base::do_swap(g);
  std::swap(acquired_in_tx, g.acquired_in_tx);
}

template <class IR>
template <class T, class MarkedPtr>
bool reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::in_tx() {
  return acquired_in_tx = local_thread_data.in_tx();
}

template <class IR>
template <class T, class MarkedPtr>
bool reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::in_tx_like(const guard_ptr& p) {
  return acquired_in_tx = p.acquired_in_tx;
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::maybe_commit_tx() {
  acquired_in_tx = false;
  if (!local_thread_data.teleport_state.should_commit()) {
    return;
  }
  base_acquire_all();
  local_thread_data.commit_tx();
  local_thread_data.begin_tx();
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::tx_acquire(const MarkedPtr& p) noexcept {
  this->ptr = p;
  local_thread_data.teleport_state.on_tx_acquire();
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::tx_reset() noexcept {
  if (this->ptr.get() == nullptr) {
    return;
  }
  this->ptr = nullptr;
  maybe_commit_tx();
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::tx_reclaim(Deleter d) noexcept {
  auto* p = this->ptr.get();
  tx_reset();
  base::reclaim(p, std::move(d));
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::base_acquire() {
  if (!acquired_in_tx) {
    return;
  }
  acquired_in_tx = false;

  auto p = this->ptr;
  this->ptr = nullptr;
  base::operator=(base(p));
}

template <class IR>
template <class T, class MarkedPtr>
void reclaimer_rtm<IR>::guard_ptr<T, MarkedPtr>::base_acquire_all() {
  auto cur = dl_node_base::head()->next_dl_node;
  while (cur != dl_node_base::head()) {
    static_cast<guard_ptr*>(cur)->base_acquire();
    cur = cur->next_dl_node;
  }
}

namespace {
using namespace boost::fibers::detail; // NOLINT

class TeleportState {
public:
  // inside tx

  void on_tx_acquire() {
    --distance_left;
  }

  bool should_commit() const {
    return distance_left <= 0;
  }

  // outside tx

  void before_begin_tx() {
    distance_left = teleport_limit;
  }

  void on_commit_tx() {
    ++teleport_limit;
  }

  void on_implicit_abort() {
    teleport_limit = std::max(MIN_TELEPORT_LIMIT, teleport_limit / 2);
  }

  bool is_min_teleport_limit() const {
    return teleport_limit == MIN_TELEPORT_LIMIT;
  }

private:
  const int MIN_TELEPORT_LIMIT = 20; // based on M. Herlihy

  int teleport_limit = 256; // based on M. Herlihy
  int distance_left = 0;
};

} // namespace

template <class IR>
class alignas(64) reclaimer_rtm<IR>::thread_data : aligned_object<thread_data> {
public:
  bool begin_tx() {
    static std::minstd_rand generator{std::random_device{}()};
    std::size_t collisions = 0;
    for (std::size_t retries = 0; retries < BOOST_FIBERS_RETRY_THRESHOLD; ++retries) {
      teleport_state.before_begin_tx();

      if (rtm_status::success == (status = rtm_begin())) {
        // enter critical section
        return true;
      }
      // transaction aborted
      if (rtm_status::none != (status & rtm_status::may_retry) ||
          rtm_status::none != (status & rtm_status::memory_conflict)) {
        // another logical processor conflicted with a memory address that was
        // part or the read-/write-set
        teleport_state.on_implicit_abort();

        if (BOOST_FIBERS_CONTENTION_WINDOW_THRESHOLD > collisions) {
          std::uniform_int_distribution<std::size_t> distribution{0, static_cast<std::size_t>(1)
              << (std::min)(collisions, static_cast<std::size_t>(BOOST_FIBERS_CONTENTION_WINDOW_THRESHOLD))};
          const std::size_t z = distribution(generator);
          ++collisions;
          for (std::size_t i = 0; i < z; ++i) {
            cpu_relax();
          }
        } else {
          break;
        }
      } else if (rtm_status::none != (status & rtm_status::explicit_abort) &&
          rtm_status::none == (status & rtm_status::nested_abort)) {
        abort(); // should not be any explicit/nested aborts
      } else {
        // transaction aborted due:
        //  - internal buffer to track transactional state overflowed
        //  - debug exception or breakpoint exception was hit
        //  - abort during execution of nested transactions (max nesting limit exceeded)
        // -> use fallback path
        if (teleport_state.is_min_teleport_limit()) { // definitely not capacity abort
          break;
        }
        teleport_state.on_implicit_abort(); // maybe capacity abort
      }
    }
    return false;
  }

  void commit_tx() {
    rtm_end();
    status = rtm_status::none;

    teleport_state.on_commit_tx();
  }

  bool in_tx() const {
    return status == rtm_status::success;
  }

  bool ensure_in_tx() {
    return in_tx() || begin_tx();
  }

public:
  TeleportState teleport_state;

private:
  std::uint32_t status = rtm_status::none;

  ALLOCATION_COUNTER(reclaimer_rtm);
};

#ifdef TRACK_ALLOCATIONS
template <class InnerReclaimer>
inline void reclaimer_rtm<InnerReclaimer>::count_allocation() {
  local_thread_data.allocation_counter.count_allocation();
}

template <class InnerReclaimer>
inline void reclaimer_rtm<InnerReclaimer>::count_reclamation() {
  local_thread_data.allocation_counter.count_reclamation();
}
#endif
} // namespace xenium::reclamation

#ifdef _MSC_VER
#pragma warning(pop)
#endif

