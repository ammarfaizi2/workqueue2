// SPDX-License-Identifier: GPL-2.0-only
/*
 *
 * Copyright (C) 2022-2023  Ammar Faizi <ammarfaizi2@gnuweeb.org>
 *
 */
#include "WorkQueue.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <cassert>

using namespace std::chrono_literals;

#ifndef __hot
#define __hot		__attribute__((__hot__))
#endif

#ifndef __cold
#define __cold		__attribute__((__cold__))
#endif

#ifdef __CHECKER__
#define __must_hold(x)		__attribute__((context(x,1,1)))
#define __acquires(x)		__attribute__((context(x,0,1)))
#define __cond_acquires(x)	__attribute__((context(x,0,-1)))
#define __releases(x)		__attribute__((context(x,1,0)))
#else /* #ifdef __CHECKER__ */
#define __must_hold(X)
#define __acquires(X)
#define __releases(X)
#define __cond_acquires(X)
#endif /* #ifdef __CHECKER__ */

#ifndef __always_inline
#define __always_inline __attribute__((__always_inline__)) inline
#endif

#ifndef noinline
#define noinline __attribute__((__noinline__))
#endif

#ifndef likely
#define likely(COND)	__builtin_expect(!!(COND), 1)
#endif

#ifndef unlikely
#define unlikely(COND)	__builtin_expect(!!(COND), 0)
#endif

inline WorkQueue::WorkerThread::WorkerThread(void):
	thread_(nullptr),
	always_idle_(false)
{
	state_.store(THREAD_DEAD, std::memory_order_relaxed);
}

inline WorkQueue::WorkerThread::~WorkerThread(void)
{
	if (thread_) {
		thread_->join();
		delete thread_;
	}
}

inline void WorkQueue::WorkerThread::SetState(wstate_t st)
{
#if defined(__linux__)
	char name[64];
	char state;
	char idle;

	if (unlikely(!thread_))
		goto out;

	switch (st) {
	case THREAD_DEAD:
		state = 'O';
		break;
	case THREAD_INTERRUPTIBLE:
		state = 'S';
		break;
	case THREAD_UNINTERRUPTIBLE:
		state = 'D';
		break;
	case THREAD_ZOMBIE:
		state = 'Z';
		break;
	default:
		state = '?';
		break;
	}

	idle = (always_idle_ ? 'i' : 'e');
	snprintf(name, sizeof(name), "wq-%c%c-%u", idle, state, index_);
	pthread_setname_np(thread_->native_handle(), name);
out:
#endif /* #if defined(__linux__) */

	state_.store(st, std::memory_order_acquire);
}

inline WorkQueue::wstate_t WorkQueue::WorkerThread::GetState(void)
{
	return state_.load(std::memory_order_acquire);
}

inline void WorkQueue::Queue::Lock(void)
	__acquires(&lock_)
{
	lock_.lock();
}

inline void WorkQueue::Queue::Unlock(void)
	__releases(&lock_)
{
	lock_.unlock();
}

inline WorkQueue::Queue::Queue(uint32_t entries):
	head_(0),
	tail_(0),
	nr_cond_wait_(0),
	arr_(nullptr)
{
	uint32_t i = 2;

	while (i < entries)
		i *= 2;

	mask_ = i - 1;
}

inline WorkQueue::Queue::~Queue(void)
{
	if (arr_)
		delete[] arr_;
}

inline int WorkQueue::Queue::Init(void)
{
	uint32_t max_work = mask_ + 1;

	arr_ = new(std::nothrow) struct Work[max_work];
	if (unlikely(!arr_))
		return -ENOMEM;

	return 0;
}

inline uint32_t WorkQueue::Queue::GetSize(void)
{
	int64_t head = static_cast<int64_t>(head_);
	int64_t tail = static_cast<int64_t>(tail_);

	return llabs(tail - head);
}

inline int WorkQueue::Queue::__Push(const struct Work *w)
	__must_hold(&lock_)
{
	uint32_t size;

	size = GetSize();
	if (unlikely(size == (mask_ + 1u)))
		return -EAGAIN;

	arr_[tail_++ & mask_] = *w;
	return 0;
}

inline int WorkQueue::Queue::__Pop(struct Work *w)
	__must_hold(&lock_)
{
	uint32_t size;

	size = GetSize();
	if (unlikely(size == 0))
		return -EAGAIN;

	*w = arr_[head_++ & mask_];
	return 0;
}

inline int WorkQueue::Queue::Push(const struct Work *w)
{
	int ret;

	Lock();
	ret = __Push(w);
	Unlock();
	return ret;
}

inline int WorkQueue::Queue::Pop(struct Work *w)
{
	int ret;

	Lock();
	ret = __Pop(w);
	Unlock();
	return ret;
}

inline void WorkQueue::Stack::Lock(void)
	__acquires(&lock_)
{
	lock_.lock();
}

inline void WorkQueue::Stack::Unlock(void)
	__releases(&lock_)
{
	lock_.unlock();
}

inline WorkQueue::Stack::Stack(uint32_t max_sp):
	sp_(max_sp),
	max_sp_(max_sp),
	arr_(nullptr)
{
}

inline WorkQueue::Stack::~Stack(void)
{
	if (arr_)
		delete[] arr_;
}

inline int WorkQueue::Stack::__Push(uint32_t val)
{
	if (unlikely(sp_ == 0))
		return -EAGAIN;

	arr_[--sp_] = val;
	return 0;
}

inline int WorkQueue::Stack::__Pop(uint32_t *out)
{
	if (unlikely(sp_ == max_sp_))
		return -EAGAIN;

	*out = arr_[sp_++];
	return 0;
}

inline int WorkQueue::Stack::Push(uint32_t val)
{
	int ret;

	Lock();
	ret = __Push(val);
	Unlock();
	return ret;
}

inline int WorkQueue::Stack::Pop(uint32_t *out)
{
	int ret;

	Lock();
	ret = __Pop(out);
	Unlock();
	return ret;
}

inline int WorkQueue::Stack::Init(void)
{
	uint32_t i;

	arr_ = new(std::nothrow) uint32_t[max_sp_];
	if (unlikely(!arr_))
		return -ENOMEM;

	i = max_sp_ - 1;

	while (i--) {
		int tmp;

		tmp = __Push(i);
		assert(tmp == 0);
		(void)tmp;
	}

	return 0;
}

__cold
WorkQueue::WorkQueue(uint32_t max_work, uint32_t max_thread,
		     uint32_t min_idle_thread) noexcept:
	stop_(false),
	sched_blocked_(false),
	max_thread_(max_thread),
	min_idle_thread_(min_idle_thread),
	idle_thread_timeout_(30000),
	queue_(max_work),
	stack_(max_thread),
	workers_(nullptr),
	nr_sched_cond_wait_(0)
{
	nr_task_uninterruptible_.store(0, std::memory_order_relaxed);
}

__cold
WorkQueue::~WorkQueue(void) noexcept
{
	stop_ = true;

	if (workers_) {
		queue_.Lock();
		queue_.cond_.notify_all();
		WakePendingScheduleCaller();
		queue_.Unlock();
		delete[] workers_;
	}

	queue_.Lock();
	while (1) {
		struct Work w;

		if (queue_.__Pop(&w))
			break;

		if (w.data_deleter_)
			w.data_deleter_(w.data_);
	}
	queue_.Unlock();

	if (sched_blocked_) {
		wait_uninterruptible_lock_.lock();
		wait_uninterruptible_cond_.notify_all();
		wait_uninterruptible_lock_.unlock();
	}
}

inline void WorkQueue::WakePendingScheduleCaller(bool always_one) noexcept
	__must_hold(&queue_.lock_)
{
	if (!nr_sched_cond_wait_)
		return;

	if (always_one || nr_sched_cond_wait_ == 1)
		sched_cond_.notify_one();
	else
		sched_cond_.notify_all();
}

/*
 * "return false" means the worker should exit.
 * "return true"  means the worker should keep running.
 */
bool WorkQueue::WaitForQueueReady(struct WorkerThread *wt,
				  std::unique_lock<std::mutex> &lock) noexcept
	__must_hold(&queue_.lock_)
{
	bool ret;

	queue_.nr_cond_wait_++;
	if (wt->always_idle_) {
		queue_.cond_.wait(lock);
		ret = true;
	} else {
		auto tmp = queue_.cond_.wait_for(lock,
						 idle_thread_timeout_ * 1ms);
		ret = (tmp != std::cv_status::timeout);
	}
	queue_.nr_cond_wait_--;
	return ret;
}

__hot inline
struct Work *WorkQueue::GetWorkFromQueue(struct WorkerThread *wt) noexcept
{
	std::unique_lock<std::mutex> lock(queue_.lock_);

	while (1) {
		struct Work *w = &wt->work_;

		if (unlikely(stop_))
			return nullptr;

		if (likely(!queue_.__Pop(w))) {
			wt->SetState(THREAD_UNINTERRUPTIBLE);
			nr_task_uninterruptible_.fetch_add(1u);
			WakePendingScheduleCaller(true);
			return w;
		}

		if (unlikely(!WaitForQueueReady(wt, lock)))
			return nullptr;
	}
}

__hot
noinline void WorkQueue::RunWorker(struct WorkerThread *wt) noexcept
	__must_hold(&wt->lock_)
{
	struct Work *w;

	while (1) {
		w = GetWorkFromQueue(wt);
		if (!w)
			break;

		if (likely(!stop_))
			w->func_(w->data_);

		if (w->data_deleter_)
			w->data_deleter_(w->data_);

		w->Clear();
		wt->SetState(THREAD_INTERRUPTIBLE);
		nr_task_uninterruptible_.fetch_sub(1u);
		if (unlikely(sched_blocked_)) {
			wait_uninterruptible_lock_.lock();
			wait_uninterruptible_cond_.notify_all();
			wait_uninterruptible_lock_.unlock();
		}
	}

	PutWorker(wt);
}

noinline void WorkQueue::PutWorker(struct WorkerThread *wt) noexcept
{
	int tmp;

	if (wt->thread_)
		wt->SetState(THREAD_ZOMBIE);
	else
		wt->SetState(THREAD_DEAD);

	tmp = stack_.Push(wt->index_);
	assert(tmp == 0);
	(void)tmp;
}

noinline int WorkQueue::StartWorker(struct WorkerThread *wt)
{
	std::unique_lock<std::mutex> lock(wt->lock_);
	std::thread *t;
	uint32_t state;

	state = wt->GetState();
	if (wt->thread_) {
		assert(state == THREAD_ZOMBIE);
		wt->thread_->join();
		delete wt->thread_;
		wt->thread_ = nullptr;
	} else {
		assert(state == THREAD_DEAD);
	}

	(void)state;

#if defined(__cpp_exceptions)
	try {
#endif
		/*
		 * TODO(ammarfaizi2):
		 * Ensure the std::thread here won't throw any exceptions.
		 * If it may throw, make sure we handle it with a
		 * try-and-catch statement. The caller should not care
		 * about the exception. We want to convert the exception
		 * to an error code.
		 *
		 * IIRC, std::thread may throw an OS error with -EAGAIN if
		 * it fails to create an OS thread. Of course! That's what
		 * the pthread_create() does too, right?
		 */
		t = new(std::nothrow) std::thread([this, wt](){
			wt->lock_.lock();
			assert(wt->thread_);
			assert(wt->GetState() == THREAD_INTERRUPTIBLE);
			RunWorker(wt);
			wt->lock_.unlock();
		});

#ifdef __cpp_exceptions
	} catch (const std::system_error& e) {
		int err = e.code().value();

		if (t)
			delete t;

		return err;
	}
#endif

	if (unlikely(!t))
		return -ENOMEM;

	wt->thread_ = t;
	wt->SetState(THREAD_INTERRUPTIBLE);
	return 0;
}

inline struct WorkQueue::WorkerThread *WorkQueue::GetWorker(void)
{
	struct WorkerThread *wt;
	uint32_t index;
	int tmp;

	tmp = stack_.Pop(&index);
	if (unlikely(tmp))
		return nullptr;

	wt = &workers_[index];
	assert(wt->index_ == index);
	return wt;
}

__cold inline int WorkQueue::InitWorkers(void)
{
	uint32_t i;
	int ret;

	if (min_idle_thread_ > max_thread_)
		min_idle_thread_ = max_thread_;

	workers_ = new(std::nothrow) WorkerThread[max_thread_];
	if (unlikely(!workers_))
		return -ENOMEM;

	ret = stack_.Init();
	if (unlikely(ret))
		return ret;

	for (i = 0; i < max_thread_; i++) {
		struct WorkerThread *w;

		w = &workers_[i];
		w->index_ = i;
	}

	for (i = 0; i < min_idle_thread_; i++) {
		struct WorkerThread *w;

		w = GetWorker();
		assert(w != nullptr);
		w->always_idle_ = true;
		ret = StartWorker(w);
		if (unlikely(ret))
			return ret;
	}

	return 0;
}

__cold inline int WorkQueue::InitWorkArray(void)
{
	int ret;

	ret = queue_.Init();
	if (unlikely(ret))
		return ret;

	return 0;
}

__cold
int WorkQueue::Init(void) noexcept
{
	int ret;

	ret = InitWorkers();
	if (ret)
		return ret;

	ret = InitWorkArray();
	if (ret)
		return ret;

	return 0;
}

inline void WorkQueue::SpawnMoreWorker(void)
{
	struct WorkerThread *wt;

	wt = GetWorker();
	if (!wt) {
		/*
		 * We hit the thread limit. Do nothing.
		 */
		return;
	}

	if (StartWorker(wt)) {
		/*
		 * Fail to start the worker, but our limit
		 * hasn't been reached. Something wrong.
		 *
		 * Either OS error (ENOMEM or EAGAIN).
		 *
		 * Put the resource back.
		 */
		PutWorker(wt);
	}
}

__hot
__always_inline int WorkQueue::RawScheduleWork(struct Work *w,
					       bool wait_if_queue_full)
{
	std::unique_lock<std::mutex> lock(queue_.lock_);
	int ret;

	if (unlikely(sched_blocked_)) {
		/*
		 * We are not allowed to schedule work.
		 * Someone is calling WaitAll() right now!
		 */
		return -EAGAIN;
	}

	while (1) {
		if (unlikely(stop_)) {
			/*
			 * The destructor has been called. It's safe to
			 * operate on this function because we are
			 * holding @queue_.lock_ at this point. The
			 * destructor is waiting for us to relase the
			 * lock.
			 */
			ret = -EOWNERDEAD;
			break;
		}

		if (likely(!queue_.__Push(w))) {
			/*
			 * Good, the work is scheduled!
			 */
			ret = 0;
			break;
		}

		if (!wait_if_queue_full) {
			/*
			 * The queue is full, but the caller doesn't
			 * want to wait. Return -EAGAIN now!
			 */
			ret = -EAGAIN;
			break;
		}

		nr_sched_cond_wait_++;
		sched_cond_.wait(lock);
		nr_sched_cond_wait_--;
	}

	if (unlikely(ret)) {
		/*
		 * Failed to schedule work.
		 */
		return ret;
	}

	if (queue_.nr_cond_wait_ > 0) {
		/*
		 * There are sleeping worker(s). Wake one of
		 * them so that they consume the work we have
		 * just scheduled.
		 */
		queue_.cond_.notify_one();
	} else {
		/*
		 * There are no sleeping worker(s). This means
		 * all active workers are busy. Try to spawn
		 * a new worker.
		 *
		 * If we haven't reached the max thread limit, a
		 * new worker will be created.
		 *
		 * If we have reached the max thread limit, this
		 * will fail. But that is fine, the next free
		 * worker will pick our scheduled work.
		 */
		SpawnMoreWorker();
	}
	return 0;
}

__hot
int WorkQueue::ScheduleWork(void (*func)(void *data), void *data,
			    void (*data_deleter)(void *data),
			    bool wait_if_queue_full) noexcept
{
	struct Work w;

	w.data_ = data;
	w.func_ = func;
	w.data_deleter_ = data_deleter;
	return RawScheduleWork(&w, wait_if_queue_full);
}

void WorkQueue::WaitAll(void) noexcept
{
	std::unique_lock<std::mutex> lock_q(queue_.lock_);
	std::unique_lock<std::mutex> lock_w(wait_uninterruptible_lock_);

	sched_blocked_ = true;
	while (1) {

		/*
		 * Wait for the queue to be empty. At this point,
		 * no one can add work to the queue because
		 * @sched_blocked_ is true.
		 */

		if (stop_)
			break;

		if (queue_.GetSize() == 0)
			break;

		nr_sched_cond_wait_++;
		sched_cond_.wait(lock_q);
		nr_sched_cond_wait_--;
	}

	while (1) {

		/*
		 * Wait for all uinterruptible tasks finish.
		 */

		if (stop_)
			break;

		if (!nr_task_uninterruptible_.load(std::memory_order_acquire))
			break;

		wait_uninterruptible_cond_.wait(lock_w);
	}
	sched_blocked_ = false;
}

void WorkQueue::SetIdleThreadTimeout(uint32_t ms) noexcept
{
	std::unique_lock<std::mutex> lock(queue_.lock_);

	idle_thread_timeout_ = ms;

	if (!queue_.nr_cond_wait_)
		return;

	if (queue_.nr_cond_wait_ == 1)
		queue_.cond_.notify_one();
	else
		queue_.cond_.notify_all();
}
