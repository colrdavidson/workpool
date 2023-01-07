#include "pool.h"

#if defined(_MSC_VER)
#define ATOMIC_INC32(val) (_InterlockedIncrement(&(val)))
#else
#define ATOMIC_INC32(val) (atomic_fetch_add_explicit(&val, 1, memory_order_relaxed))
#endif


_Atomic static int total_tasks = 0;
void little_work(TPool *pool, void *args) {
	// this is my workload. enjoy

#ifndef _WIN32
	int sleep_time = rand() % 201;
	usleep(sleep_time);
#else
	static float aaa[10000];
	for (size_t i = 0; i < 10000; i++) {
		aaa[i] = (rand() % 2000) * 0.25;
	}
#endif

	if (total_tasks < 2000) {
		for (int i = 0; i < 5; i++) {
			TPool_Task task;
			task.do_work = little_work;
			task.args = NULL;
			tpool_add_task(pool, task);
		}
	}

	ATOMIC_INC32(total_tasks);
}

int main(void) {
	srand(1);

	TPool pool = {0};
	tpool_init(&pool, 32);

	int initial_task_count = 10;
	for (int i = 0; i < initial_task_count; i++) {
		TPool_Task task;
		task.do_work = little_work;
		task.args = NULL;
		tpool_add_task(&pool, task);
	}
	tpool_wait(&pool);

	total_tasks = 0;
	for (int i = 0; i < initial_task_count; i++) {
		TPool_Task task;
		task.do_work = little_work;
		task.args = NULL;
		tpool_add_task(&pool, task);
	}
	tpool_wait(&pool);

	total_tasks = 0;
	for (int i = 0; i < initial_task_count; i++) {
		TPool_Task task;
		task.do_work = little_work;
		task.args = NULL;
		tpool_add_task(&pool, task);
	}
	tpool_wait(&pool);
	tpool_destroy(&pool);
}
