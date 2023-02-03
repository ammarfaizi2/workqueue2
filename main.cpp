
#include "WorkQueue.h"
#include <unistd.h>

static std::atomic<uint32_t> x = 0;

static void test_func(void *data)
{
	x.fetch_add(1);
	__asm__ volatile ("nop");
	(void)data;
}

int main(void)
{
	const uint32_t max_work = 10240 * 10;
	const uint32_t max_thread = 128;
	const uint32_t min_idle_thread = 3;
	WorkQueue wq(max_work, max_thread, min_idle_thread);
	uint32_t i;
	int ret;

	ret = wq.Init();
	if (ret < 0) {
		printf("Cannot allocate memory!\n");
		return ret;
	}

	for (i = 0; i < max_work; i++)
		wq.ScheduleWork(test_func);

	wq.WaitAll();
	printf("x = %u\n", x.load());
	return 0;
}
