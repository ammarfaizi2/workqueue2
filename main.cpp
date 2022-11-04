
#include <unistd.h>
#include <cassert>
#include "WorkQueue.h"

using Wq::WorkQueue;

static std::atomic<uint32_t> g_counter;

void work_func(void *data_)
{
	(void)data_;
	sleep(1);
	g_counter.fetch_add(1);
}

void data_deleter(void *data)
{
	delete static_cast<int *>(data);
}

int main(void)
{
	uint32_t total_schedule = 0;
	WorkQueue *wq;
	int iter = 3;
	int *data;
	int ret;
	int i;

	wq = new WorkQueue(10240, 10240);
	ret = wq->Init();
	if (ret)
		return -ret;

	g_counter.store(0);
	while (iter--) {
		wq->SetWQIdleSeconds(3);
		for (i = 0; i < 10240; i++) {
			data = new int;
			*data = i;
			ret = wq->ScheduleWork(work_func, data, data_deleter);
			if (!ret)
				total_schedule++;
		}
		wq->WaitAll();
		printf("Sleeping for 5 seconds...\n");
		sleep(5);
	}

	assert(g_counter.load() == total_schedule);
	delete wq;
	return 0;
}
