// SPDX-License-Identifier: GPL-2.0-only
/*
 *
 * Copyright (C) 2022  Ammar Faizi <ammarfaizi2@gnuweeb.org>
 *
 */

#ifndef WQ_WORKQUEUE_H
#define WQ_WORKQUEUE_H

#include <thread>
#include <mutex>
#include <cerrno>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <condition_variable>

#ifndef likely
#define likely(COND)	__builtin_expect(!!(COND), 1)
#endif

#ifndef unlikely
#define unlikely(COND)	__builtin_expect(!!(COND), 0)
#endif

#ifndef __hot
#define __hot		__attribute__((__hot__))
#endif

#ifndef __cold
#define __cold		__attribute__((__cold__))
#endif

#define __WRITE_ONCE(x, val)				\
do {							\
	*(volatile __typeof__(x) *)&(x) = (val);	\
} while (0)

#define WRITE_ONCE(x, val)	\
do {				\
	__WRITE_ONCE(x, val);	\
} while (0)

#ifndef __READ_ONCE
#define __READ_ONCE(x)	(*(const volatile __typeof__(x) *)&(x))
#endif

#ifndef READ_ONCE
#define READ_ONCE(x) (__READ_ONCE(x))
#endif

#ifndef __must_hold
#define __must_hold(LOCK)
#endif

#ifndef __acquires
#define __acquires(LOCK)
#endif

#ifndef __releases
#define __releases(LOCK)
#endif

#ifndef noinline
#define noinline	__attribute__((__noinline__))
#endif

namespace Wq {

template<typename T>
class Queue {
public:
	inline Queue(uint32_t want_max)
	{
		uint32_t max = 1;

		while (max < want_max)
			max *= 2;

		mask_ = max - 1;
		head_ = 0;
		tail_ = 0;
		arr_  = nullptr;
	}

	inline int Init(void)
	{
		arr_ = static_cast<T *>(malloc(sizeof(*arr_) * (mask_ + 1)));
		if (!arr_)
			return -ENOMEM;

		return 0;
	}

	inline ~Queue(void)
	{
		if (arr_)
			free(arr_);
	}

	inline uint32_t GetSize(void)
	{
		int64_t head = static_cast<int64_t>(head_);
		int64_t tail = static_cast<int64_t>(tail_);

		return llabs(tail - head);
	}

	inline uint32_t GetMaxSize(void)
	{
		return mask_ + 1u;
	}

	inline int Push(const T *t)
	{
		if (unlikely(GetSize() == GetMaxSize()))
			/*
			 * The queue is full.
			 */
			return -EAGAIN;

		arr_[tail_++ & mask_] = *t;
		return 0;
	}

	inline int Pop(T *out)
	{
		if (unlikely(head_ == tail_))
			/*
			 * The queue is empty.
			 */
			return -EAGAIN;

		*out = arr_[head_++ & mask_];
		return 0;
	}

private:
	uint32_t	mask_;
	uint32_t	head_;
	uint32_t	tail_;
	T		*arr_;
};

template<typename T>
class Stack {
public:
	inline Stack(uint32_t max)
	{
		esp_ = max;
		max_ = max;
		arr_ = nullptr;
	}

	inline ~Stack(void)
	{
		if (arr_)
			free(arr_);
	}

	inline int Init(void)
	{
		arr_ = static_cast<T *>(malloc(sizeof(*arr_) * max_));
		if (unlikely(!arr_))
			return -ENOMEM;

		return 0;
	}

	inline int Push(T t)
	{
		arr_[--esp_] = t;
		return 0;
	}

	inline int Pop(T *out)
	{
		if (unlikely(esp_ == max_))
			return -EAGAIN;

		*out = arr_[esp_++];
		return 0;
	}

private:
	uint32_t	max_;
	uint32_t	esp_;
	T		*arr_;
};

struct Work {
	void		*data_;
	void		(*func_)(void *data);
	void		(*deleter_)(void *data);
	unsigned	flags_;
};

enum WorkerThreadState {
	THREAD_DEAD		= (1u << 0u),
	THREAD_UNINTERRUPTIBLE	= (1u << 1u),
	THREAD_INTERRUPTIBLE	= (1u << 2u),
	THREAD_ZOMBIE		= (1u << 3u),
};

struct WorkerThread {
	struct Work				work_;
	std::atomic<std::thread *>		thread_;
	std::atomic<enum WorkerThreadState>	state_;
	std::mutex				lock_;
	uint32_t				idx_;
	bool					idle_;

	inline WorkerThread(void)
	{
		memset(&work_, 0, sizeof(work_));
		thread_.store(nullptr, std::memory_order_relaxed);
		state_.store(THREAD_DEAD, std::memory_order_relaxed);
	}

	inline bool HasWork(void)
	{
		return (work_.func_ != nullptr);
	}

	inline enum WorkerThreadState GetState(void)
	{
		return state_.load(std::memory_order_acquire);
	}

	void SetState(enum WorkerThreadState st) noexcept;
};

class WorkQueue {
public:
	WorkQueue(uint32_t nr_max_work = 128, uint32_t nr_max_thread = 64,
		  uint32_t nr_min_idle_thread = 1) noexcept;

	~WorkQueue(void) noexcept;

	int Init(void) noexcept;

	int ScheduleWork(void (*func)(void *data), void *data = nullptr,
			 void (*deleter)(void *data) = nullptr) noexcept;

	int TryScheduleWork(void (*func)(void *data), void *data = nullptr,
			    void (*deleter)(void *data) = nullptr) noexcept;

	void WaitAll(void) noexcept;

	inline bool ShouldStop(void)
	{
		return unlikely(stop_.load(std::memory_order_acquire));
	}

	inline void SetWQIdleSeconds(uint32_t secs)
	{
		wq_idle_seconds_ = secs;
	}

private:
	std::atomic<bool>		stop_;
	std::atomic<bool>		enqueue_blocked_;

	struct WorkerThread		*workers_;
	Queue<struct Work>		queue_;
	Stack<uint32_t>			free_worker_idx_;
	uint32_t			nr_max_thread_;
	uint32_t			nr_min_idle_thread_;

	std::atomic<uint32_t>		nr_online_threads_;
	std::atomic<uint32_t>		nr_online_idle_threads_;
	std::atomic<uint32_t>		nr_schedule_call_waiting_;

	uint32_t			wq_idle_seconds_ = 300;

	std::mutex			queue_lock_;
	std::condition_variable		queue_cond_;
	std::condition_variable		schedule_cond_;
	std::condition_variable		wait_all_cond_;
	std::mutex			workers_lock_;

	inline bool IsEnqueueBlocked(void)
	{
		return enqueue_blocked_.load(std::memory_order_acquire);
	}

	int RawScheduleWork(struct Work *w);
	struct WorkerThread *GetWorker(void);
	void PutWorker(struct WorkerThread *wt);
	void SpawnWorkerIfExhausted(std::unique_lock<std::mutex> &qlock);

	void NotifyScheduleCall(void);
	void NotifyWaitAllCall(void);

	int GrabWorkWait(struct WorkerThread *wt,
			 std::unique_lock<std::mutex> &lk);
	int GrabWork(struct WorkerThread *wt);
	int DoWork(struct WorkerThread *wt);

	void StartWorkerThreadIdle(struct WorkerThread *wt);
	void StartWorkerThreadExtra(struct WorkerThread *wt);
	void StartWorker(struct WorkerThread *wt);

	int SpawnWorker(struct WorkerThread *wt, bool idle);
	int SpawnWorkerExtra(struct WorkerThread *wt);
	int SpawnWorkerIdle(struct WorkerThread *wt);
	int InitWorkers(void);

	void DestroyQueue(void);
	void DestroyWorker(void);

	inline void IncScheduleCallWaiting(void)
	{
		nr_schedule_call_waiting_.fetch_add(1u, std::memory_order_release);
	}

	inline void DecScheduleCallWaiting(void)
	{
		nr_schedule_call_waiting_.fetch_sub(1u, std::memory_order_release);
	}

	inline uint32_t GetNRScheduleCallWaiting(void)
	{
		return nr_schedule_call_waiting_.load(std::memory_order_acquire);
	}

	inline uint32_t GetNrOnlineThread(void)
	{
		return nr_online_threads_.load(std::memory_order_acquire);
	}

	inline void IncOnline(void)
	{
		nr_online_threads_.fetch_add(1u, std::memory_order_release);
	}

	inline void DecOnline(void)
	{
		nr_online_threads_.fetch_sub(1u, std::memory_order_release);
	}

	inline void IncOnlineIdle(void)
	{
		nr_online_idle_threads_.fetch_add(1u, std::memory_order_release);
	}

	inline void DecOnlineIdle(void)
	{
		nr_online_idle_threads_.fetch_sub(1u, std::memory_order_release);
	}

	inline uint32_t GetNrIdleThreads(void)
	{
		return nr_online_idle_threads_.load(std::memory_order_acquire);
	}
};

} /* namespace Wq */

#endif /* #ifndef WQ_WORKQUEUE_H */
