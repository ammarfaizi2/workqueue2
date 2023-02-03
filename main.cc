
#include "workqueue.h"
#include <cstdio>
#include <cerrno>
#include <cstring>
#include <atomic>
#include <unistd.h>

static std::atomic<uint32_t> gn;

void f(void *)
{
	gn++;
}

int main(void)
{
	struct workqueue_attr attr;
	struct workqueue_struct *wq;
	uint64_t i;
	int ret;

	memset(&attr, 0, sizeof(attr));
	attr.min_threads = 0;
	attr.max_threads = 8;
	attr.max_pending_works = 1024;
	ret = alloc_workqueue(&wq, &attr);
	if (ret) {
		fprintf(stderr, "workqueue_create failed: %d\n", ret);
		return ret;
	}

	for (i = 0; i < 5; i++) {
		ret = queue_work(wq, f, NULL, NULL);
		if (ret) {
			fprintf(stderr, "queue_work failed: %d\n", ret);
			break;
		}
	}

	wait_all_work_done(wq);
	printf("gn = %u\n", gn.load());
	destroy_workqueue(wq);
	return 0;
}
