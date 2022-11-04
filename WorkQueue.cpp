// SPDX-License-Identifier: GPL-2.0-only
/*
 *
 * Copyright (C) 2022  Ammar Faizi <ammarfaizi2@gnuweeb.org>
 *
 */

#include <chrono>
#include <cassert>
#include <cstring>

#include "WorkQueue.h"

using namespace std::chrono_literals;

namespace Wq {

WorkQueue::WorkQueue(uint32_t nr_max_work, uint32_t nr_max_thread,
		     uint32_t nr_min_idle_thread) noexcept:
	workers_(nullptr),
	queue_(nr_max_work),
	free_worker_idx_(nr_max_thread),
	nr_max_thread_(nr_max_thread),
	nr_min_idle_thread_(nr_min_idle_thread)
{
	stop_.store(false, std::memory_order_relaxed);
	enqueue_blocked_.store(false, std::memory_order_relaxed);
	nr_online_threads_.store(0u, std::memory_order_relaxed);
	nr_online_idle_threads_.store(0u, std::memory_order_relaxed);
	nr_schedule_call_waiting_.store(0u, std::memory_order_relaxed);
}

inline int WorkQueue::RawScheduleWork(struct Work *w)
	__must_hold(&queue_lock_)
{
	/*
	 * TODO(ammarfaizi2): Add tracing feature.
	 */
	return queue_.Push(w);
}

/*
 * Try to schedule a task work, if we can't acquire the
 * mutex or the queue is full, it returns -EAGAIN. This
 * function will not sleep.
 */
int WorkQueue::TryScheduleWork(void (*func)(void *data), void *data,
			       void (*deleter)(void *data)) noexcept
{
	struct Work w;
	int ret;

	if (unlikely(!func))
		return -EINVAL;

	if (ShouldStop())
		return -EOWNERDEAD;

	if (unlikely(IsEnqueueBlocked()))
		return -EAGAIN;

	if (unlikely(!queue_lock_.try_lock()))
		return -EAGAIN;

	w.func_ = func;
	w.data_ = data;
	w.deleter_ = deleter;
	ret = RawScheduleWork(&w);
	queue_lock_.unlock();

	if (likely(!ret))
		queue_cond_.notify_one();

	return ret;
}

/*
 * Schedule a task work. If the queue is full, it will
 * be sleeping and retrying to enqueue the task work
 * until it succeeds.
 *
 * If the destructor is called (or ShouldStop() returns
 * true) before it manages to put the task work into
 * the queue, it cancels the submission and returns
 * -EOWNERDEAD.
 */
int WorkQueue::ScheduleWork(void (*func)(void *data), void *data,
			    void (*deleter)(void *data)) noexcept
{
	struct Work w;
	int ret;

	if (unlikely(!func))
		return -EINVAL;

	if (ShouldStop())
		return -EOWNERDEAD;

	if (unlikely(IsEnqueueBlocked()))
		return -EAGAIN;

	w.func_ = func;
	w.data_ = data;
	w.deleter_ = deleter;

	if (!GetNrIdleThreads()) {
		struct WorkerThread *wt = GetWorker();

		if (wt) {
			wt->work_ = w;
			ret = SpawnWorkerExtra(wt);
			if (!ret)
				return 0;
		}
	}

	std::unique_lock<std::mutex> lk(queue_lock_);

	while (1) {
		if (ShouldStop())
			return -EOWNERDEAD;

		ret = queue_.Push(&w);
		if (!ret)
			break;

		IncScheduleCallWaiting();
		schedule_cond_.wait(lk);
		DecScheduleCallWaiting();
	}

	if (likely(!ret))
		queue_cond_.notify_one();

	return ret;
}

/*
 * Wait for all pending task works get executed. When
 * waiting, if another thread schedules a task work,
 * the submission will fail with -EAGAIN.
 */
void WorkQueue::WaitAll(void) noexcept
{
	assert(!IsEnqueueBlocked());

	std::unique_lock<std::mutex> lk(queue_lock_);
	enqueue_blocked_.store(true, std::memory_order_release);
	while (1) {
		if (!queue_.GetSize())
			break;

		if (ShouldStop())
			break;

		wait_all_cond_.wait(lk);
	}
	enqueue_blocked_.store(false, std::memory_order_release);
}

inline void WorkQueue::PutWorker(struct WorkerThread *wt)
{
	assert(!wt->HasWork());

	std::unique_lock<std::mutex> lk(workers_lock_);
	assert(free_worker_idx_.Push(wt->idx_) == 0);
}

struct WorkerThread *WorkQueue::GetWorker(void)
{
	struct WorkerThread *ret = nullptr;
	uint32_t idx;

	if (ShouldStop())
		return ret;

	std::unique_lock<std::mutex> lk(workers_lock_);
	if (likely(!free_worker_idx_.Pop(&idx))) {
		ret = &workers_[idx];
		assert(!ret->HasWork());
		assert(ret->idx_ == idx);
	}
	return ret;
}

inline int WorkQueue::GrabWorkWait(struct WorkerThread *wt,
				   std::unique_lock<std::mutex> &lk)
	__must_hold(&queue_lock_)
{
	if (wt->idle_) {
		queue_cond_.wait(lk);
		return 0;
	}

	auto ret = queue_cond_.wait_for(lk, 1s);
	if (ret == std::cv_status::timeout)
		return -ETIME;

	return 0;
}

inline void WorkQueue::NotifyWaitAllCall(void)
	__must_hold(&queue_lock_)
{
	if (queue_.GetSize() > 0)
		return;

	if (enqueue_blocked_.load(std::memory_order_acquire))
		wait_all_cond_.notify_one();
}

inline void WorkQueue::NotifyScheduleCall(void)
	__must_hold(&queue_lock_)
{
	if (GetNRScheduleCallWaiting())
		schedule_cond_.notify_one();
}

inline
void WorkQueue::SpawnWorkerIfExhausted(std::unique_lock<std::mutex> &qlock)
	__must_hold(&queue_lock_)
{
	struct WorkerThread *wt;
	bool pop_ok;
	int ret;

	if (likely(queue_.GetSize() < 2))
		return;

	if (GetNrOnlineThread() == nr_max_thread_)
		return;

	qlock.unlock();
	wt = GetWorker();
	qlock.lock();
	if (!wt)
		return;

	ret = queue_.Pop(&wt->work_);
	pop_ok = (ret == 0);

	ret = SpawnWorkerExtra(wt);
	if (unlikely(ret && pop_ok)) {
		/*
		 * Failed to spawn the worker, but we already popped
		 * the task work. Put it back into the queue.
		 *
		 * This push must not fail, because we are still
		 * holding the @queue_lock_, so it's impossible
		 * for someone makes the queue full before us.
		 */
		assert(queue_.Push(&wt->work_) == 0);
		wt->work_.func_ = nullptr;
	}
}

__hot inline int WorkQueue::GrabWork(struct WorkerThread *wt)
{
	std::unique_lock<std::mutex> lk(queue_lock_);

	if (ShouldStop())
		return -EOWNERDEAD;

	SpawnWorkerIfExhausted(lk);
	if (wt->HasWork())
		return 0;

	while (1) {
		int ret;

		if (ShouldStop())
			return -EOWNERDEAD;

		if (!queue_.Pop(&wt->work_)) {
			NotifyScheduleCall();
			NotifyWaitAllCall();
			return 0;
		}

		ret = GrabWorkWait(wt, lk);
		if (ret)
			return ret;
	}
}

__hot noinline void WorkQueue::StartWorker(struct WorkerThread *wt)
{
	std::unique_lock<std::mutex> lk(wt->lock_);

	wt->SetState(THREAD_INTERRUPTIBLE);
	IncOnline();
	IncOnlineIdle();

	if (wt->idle_)
		StartWorkerThreadIdle(wt);
	else
		StartWorkerThreadExtra(wt);

	DecOnlineIdle();
	DecOnline();
	assert(!wt->HasWork());
	wt->SetState(THREAD_ZOMBIE);
	PutWorker(wt);
}

__hot noinline int WorkQueue::DoWork(struct WorkerThread *wt)
{
	struct Work *w;
	int ret;

	ret = GrabWork(wt);
	if (unlikely(ret)) {

		if (likely(!wt->HasWork()))
			return ret;

		w = &wt->work_;
		if (w->deleter_)
			w->deleter_(w->data_);

		w->func_ = nullptr;
		return ret;
	}

	assert(wt->HasWork());
	DecOnlineIdle();
	wt->SetState(THREAD_UNINTERRUPTIBLE);
	w = &wt->work_;

	if (unlikely(ShouldStop())) {
		/*
		 * We managed to grab a work, but the destructor
		 * has been invoked. We are exiting now...
		 */
		ret = -EOWNERDEAD;
	} else {
		/*
		 * Execute the work. This may take some time. At
		 * this point, if the destructor is invoked, the
		 * destructor will be waiting for us to finish
		 * the work.
		 *
		 * TODO(ammarfaizi2):
		 * Make it possible for the task work to cancel
		 * itself when the destructor is called while
		 * executing @w->func_.
		 */
		w->func_(w->data_);
		ret = 0;
	}

	if (w->deleter_)
		w->deleter_(w->data_);

	w->func_ = nullptr;
	wt->SetState(THREAD_INTERRUPTIBLE);
	IncOnlineIdle();
	return ret;
}

__hot noinline void WorkQueue::StartWorkerThreadIdle(struct WorkerThread *wt)
{
	int ret;

	while (1) {
		ret = DoWork(wt);
		if (unlikely(ret != 0))
			break;
	}
}

__hot noinline void WorkQueue::StartWorkerThreadExtra(struct WorkerThread *wt)
{
	uint32_t counter = 0;
	int ret;

	while (1) {
		ret = DoWork(wt);
		if (likely(ret == 0)) {
			counter = 0;
			continue;
		}

		if (unlikely(ret != -ETIME))
			break;

		if (unlikely(++counter >= wq_idle_seconds_))
			break;
	}
}

int WorkQueue::SpawnWorker(struct WorkerThread *wt, bool idle)
{
	std::unique_lock<std::mutex> lk(wt->lock_);
	std::thread *t;

	t = wt->thread_.load(std::memory_order_acquire);
	if (t) {
		assert(wt->GetState() == THREAD_ZOMBIE);
		t->join();
		delete t;
		wt->thread_.store(nullptr, std::memory_order_release);
		t = nullptr;
	}

	try {
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
		 * the pthread_create() does too.
		 */
		t = new(std::nothrow) std::thread([this, wt](){
			this->StartWorker(wt);
		});

	} catch (const std::system_error& e) {
		int err = e.code().value();

		if (t)
			delete t;

		return err;
	}

	if (unlikely(!t))
		return -ENOMEM;

	wt->idle_ = idle;
	wt->thread_.store(t, std::memory_order_release);
	return 0;
}

inline int WorkQueue::SpawnWorkerExtra(struct WorkerThread *wt)
{
	return SpawnWorker(wt, false);
}

inline int WorkQueue::SpawnWorkerIdle(struct WorkerThread *wt)
{
	return SpawnWorker(wt, true);
}

__cold inline int WorkQueue::InitWorkers(void)
{
	uint32_t i;

	workers_ = new(std::nothrow) struct WorkerThread[nr_max_thread_];
	if (!workers_)
		return -ENOMEM;

	for (i = nr_max_thread_; i--;) {
		workers_[i].idx_ = i;
		free_worker_idx_.Push(i);
	}

	for (i = 0; i < nr_min_idle_thread_; i++) {
		struct WorkerThread *wt = GetWorker();
		int ret;

		assert(wt);
		ret = SpawnWorkerIdle(wt);
		if (ret)
			return ret;
	}

	return 0;
}

__cold int WorkQueue::Init(void) noexcept
{
	int ret;

	if (nr_min_idle_thread_ > nr_max_thread_)
		return -EINVAL;

	ret = queue_.Init();
	if (ret)
		return ret;

	ret = free_worker_idx_.Init();
	if (ret)
		return ret;

	return InitWorkers();
}

inline void WorkQueue::DestroyQueue(void)
	__must_hold(&queue_lock_)
{
	/*
	 * When we're exiting, we may be holding pending task
	 * works. A task work may have a data that needs to
	 * be deleted. Call the deleter.
	 */
	while (1) {
		struct Work w;

		if (queue_.Pop(&w))
			break;

		if (w.deleter_)
			w.deleter_(w.data_);
	}
}

inline void WorkQueue::DestroyWorker(void)
	__must_hold(&workers_lock_)
{
	uint32_t i;

	if (!workers_)
		return;

	for (i = 0; i < nr_max_thread_; i++) {
		struct WorkerThread *wt = &workers_[i];
		std::thread *t;

		t = wt->thread_.load(std::memory_order_acquire);
		if (!t)
			continue;

		workers_lock_.unlock();
		t->join();
		delete t;
		assert(wt->GetState() == THREAD_ZOMBIE);
		wt->thread_.store(nullptr, std::memory_order_release);
		workers_lock_.lock();
		assert(!wt->HasWork());
	}

	delete[] workers_;
	workers_ = nullptr;
}

__cold
WorkQueue::~WorkQueue(void) noexcept
{
	stop_.store(true, std::memory_order_release);
	queue_lock_.lock();
	DestroyQueue();
	NotifyScheduleCall();
	NotifyWaitAllCall();
	queue_cond_.notify_all();
	queue_lock_.unlock();

	workers_lock_.lock();
	DestroyWorker();
	workers_lock_.unlock();
}

void WorkerThread::SetState(enum WorkerThreadState st) noexcept
{
#if defined(__linux__)
	std::thread *t;
	char name[64];
	char state;
	char idle;

	t = thread_.load(std::memory_order_acquire);
	if (!t)
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

	idle = (idle_ ? 'i' : 'e');
	snprintf(name, sizeof(name), "wq-%c%c-%u", idle, state, idx_);
	pthread_setname_np(t->native_handle(), name);
out:
#endif
	state_.store(st, std::memory_order_release);
}

} /* namespace Wq */
