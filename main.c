#include "pool.h"

#if defined(_MSC_VER)
#define ATOMIC_INC32(val) (_InterlockedIncrement(&(val)))
#else
#define ATOMIC_INC32(val) (atomic_fetch_add_explicit(&val, 1, memory_order_relaxed))
#endif

#ifdef ENABLE_TRACING
#include "spall_native_auto.h"
#endif

int thread_results[9] = {0};
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

/*
void little_work(TPool *pool, void *args) {
	thread_results[tpool_current_thread_idx] += 1;
	usleep(2);
}
*/

int main(void) {
#ifdef ENABLE_TRACING
	spall_auto_init((char *)"profile.spall");
	spall_auto_thread_init(0, SPALL_DEFAULT_BUFFER_SIZE);
#endif
	srand(1);

	TPool pool = {0};
	tpool_init(&pool, 8);

	int initial_task_count = 10;
	for (int i = 0; i < initial_task_count; i++) {
		TPool_Task task;
		task.do_work = little_work;
		task.args = NULL;
		tpool_add_task(&pool, task);
	}
	tpool_wait(&pool);

	int total_tasks = 0;
	for (int i = 0; i < 9; i++) {
		total_tasks += thread_results[i];
	}
	printf("%d\n", total_tasks);

/*
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
*/
	tpool_destroy(&pool);

#ifdef ENABLE_TRACING
	spall_auto_thread_quit();
	spall_auto_quit();
#endif
}

#ifdef ENABLE_TRACING
#define SPALL_AUTO_IMPLEMENTATION
#include "spall_native_auto.h"
#endif
