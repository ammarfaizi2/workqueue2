// SPDX-License-Identifier: GPL-2.0-only
/*
 *
 * Copyright (C) 2022-2023  Ammar Faizi <ammarfaizi2@gnuweeb.org>
 *
 */
#ifndef WORKQUEUE__WORKQUEUE_H
#define WORKQUEUE__WORKQUEUE_H

#include <mutex>
#include <cerrno>
#include <atomic>
#include <thread>
#include <cstdint>
#include <functional>
#include <condition_variable>

struct Work {
	void			(*func_)(void *data);
	void			*data_;
	void			(*data_deleter_)(void *data);
	uint32_t		flags_;
	uint32_t		__resv;

	Work(void) noexcept;

	~Work(void) noexcept;

	void Clear(void) noexcept;
};

inline Work::Work(void) noexcept:
	func_(nullptr),
	data_(nullptr),
	data_deleter_(nullptr),
	flags_(0),
	__resv(0)
{
}

inline Work::~Work(void) noexcept
{
}

inline void Work::Clear(void) noexcept
{
	func_ = nullptr;
	data_ = nullptr;
	data_deleter_ = nullptr;
	flags_ = 0;
}

class WorkQueue {
public:
	WorkQueue(uint32_t max_work = 128, uint32_t max_thread = 64,
		  uint32_t min_idle_thread = 1) noexcept;

	~WorkQueue(void) noexcept;

	int Init(void) noexcept;

	int ScheduleWork(void (*func)(void *data), void *data = nullptr,
			 void (*data_deleter)(void *data) = nullptr,
			 bool wait_if_queue_full = true) noexcept;

	int TryScheduleWork(void (*func)(void *data), void *data = nullptr,
			    void (*data_deleter)(void *data) = nullptr) noexcept;

	int FScheduleWork(std::function<void(void)> func) noexcept;

	static void __FScheduleInvoke(void *data);

	static void __FScheduleDeleter(void *data) noexcept;

	void WaitAll(void) noexcept;

	void SetIdleThreadTimeout(uint32_t ms) noexcept;

private:
	enum WorkerThreadState {
		THREAD_DEAD		= (1u << 0u),
		THREAD_UNINTERRUPTIBLE	= (1u << 1u),
		THREAD_INTERRUPTIBLE	= (1u << 2u),
		THREAD_ZOMBIE		= (1u << 3u),
	};

	typedef enum WorkerThreadState wstate_t;

	struct WorkerThread {
		std::thread		*thread_;
		std::atomic<wstate_t>	state_;
		uint32_t		index_;
		struct Work		work_;
		std::mutex		lock_;
		bool			always_idle_;

		WorkerThread(void);
		~WorkerThread(void);
		void SetState(wstate_t state);
		wstate_t GetState(void);
		int StartWorker(void);
	};

	struct Queue {
		uint32_t		head_;
		uint32_t		tail_;
		uint32_t		mask_;
		uint32_t		nr_cond_wait_;
		struct Work		*arr_;
		std::mutex		lock_;
		std::condition_variable	cond_;

		Queue(uint32_t entries);
		~Queue(void);
		int Init(void);
		void Lock(void);
		void Unlock(void);
		uint32_t Length(void);
		int __Push(const struct Work *w);
		int __Pop(struct Work *w);
		int Push(const struct Work *w);
		int Pop(struct Work *w);
		uint32_t GetSize(void);
	};

	struct Stack {
		uint32_t		sp_;
		uint32_t		max_sp_;
		uint32_t		*arr_;
		std::mutex		lock_;

		Stack(uint32_t max_sp);
		~Stack(void);
		int Init(void);
		void Lock(void);
		void Unlock(void);
		uint32_t Length(void);
		int __Push(uint32_t val);
		int __Pop(uint32_t *out);
		int Push(uint32_t val);
		int Pop(uint32_t *out);
	};

	volatile bool		stop_;
	volatile bool		sched_blocked_;
	uint32_t		max_thread_;
	uint32_t		min_idle_thread_;
	uint32_t		idle_thread_timeout_;
	struct Queue		queue_;
	struct Stack		stack_;
	struct WorkerThread	*workers_;
	std::condition_variable	sched_cond_;
	uint32_t		nr_sched_cond_wait_;
	std::mutex		wait_uninterruptible_lock_;
	std::condition_variable	wait_uninterruptible_cond_;
	std::atomic<uint32_t>	nr_task_uninterruptible_;

	int InitWorkers(void);

	int InitWorkArray(void);

	struct WorkerThread *GetWorker(void);

	void PutWorker(struct WorkerThread *w) noexcept;

	void RunWorker(struct WorkerThread *w) noexcept;

	int StartWorker(struct WorkerThread *w);

	bool WaitForQueueReady(struct WorkerThread *wt,
			       std::unique_lock<std::mutex> &lock) noexcept;

	struct Work *GetWorkFromQueue(struct WorkerThread *wt) noexcept;

	int RawScheduleWork(struct Work *w, bool wait_if_queue_full);

	void WakePendingScheduleCaller(bool always_one = false) noexcept;

	void SpawnMoreWorker(void);

	void SetTaskUninterruptible(struct WorkerThread *wt);

	void SetTaskInterruptible(struct WorkerThread *wt);
};

struct FWork {
	std::function<void(void)>	func_;
};

inline void WorkQueue::__FScheduleInvoke(void *data)
{
	struct FWork *w = static_cast<struct FWork *>(data);

	w->func_();
}

inline void WorkQueue::__FScheduleDeleter(void *data) noexcept
{
	delete static_cast<struct FWork *>(data);
}

inline int WorkQueue::FScheduleWork(std::function<void(void)> func) noexcept
{
	struct FWork *w;

	w = new(std::nothrow) struct FWork;
	if (!w)
		return -ENOMEM;

	w->func_ = std::move(func);
	return ScheduleWork(&WorkQueue::__FScheduleInvoke, w,
			    &WorkQueue::__FScheduleDeleter);
}

inline int  WorkQueue::TryScheduleWork(void (*func)(void *data),
				       void *data,
				       void (*data_deleter)(void *data))
	noexcept
{
	return ScheduleWork(func, data, data_deleter, false);
}


#endif /* #ifndef WORKQUEUE__WORKQUEUE_H */
