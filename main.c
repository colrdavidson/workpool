#include "spall_auto.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <stdatomic.h>

typedef ssize_t tpool_task_proc(void *data);
typedef struct TPoolTask {
	tpool_task_proc  *do_work;
	void             *args;
} TPoolTask;

typedef struct Thread {
	pthread_t thread;
	int idx;

	TPoolTask *queue;
	size_t capacity;
	_Atomic uint64_t head_and_tail;

	struct TPool *pool;
} Thread;

typedef struct TPool {
	struct Thread *threads;

	int thread_count;
	_Atomic bool running;

	pthread_cond_t tasks_available;
	pthread_mutex_t task_lock;

	_Atomic int32_t tasks_left;
} TPool;

_Thread_local Thread *current_thread = NULL;
_Thread_local int work_count = 0;

void mutex_init(pthread_mutex_t *mut) {
	pthread_mutex_init(mut, NULL);
}
void mutex_lock(pthread_mutex_t *mut) {
	pthread_mutex_lock(mut);
}
void mutex_unlock(pthread_mutex_t *mut) {
	pthread_mutex_unlock(mut);
}
int mutex_trylock(pthread_mutex_t *mut) {
	return pthread_mutex_trylock(mut);
}
void cond_init(pthread_cond_t *cond) {
	pthread_cond_init(cond, NULL);
}
void cond_broadcast(pthread_cond_t *cond) {
	pthread_cond_broadcast(cond);
}
void cond_signal(pthread_cond_t *cond) {
	pthread_cond_signal(cond);
}
int cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex) {
	return pthread_cond_wait(cond, mutex);
}

void tqueue_push(Thread *thread, TPoolTask task) {
	uint64_t capture;
	uint64_t new_capture;
	do {
		capture = thread->head_and_tail;

		uint64_t mask = thread->capacity - 1;
		uint64_t head = (capture >> 32) & mask;
		uint64_t tail = ((uint32_t)capture) & mask;

		uint64_t new_head = (head + 1) & mask;
		if (new_head == tail) {
			printf("queue full? %lx %lx %lx, %lx\n", head, new_head, tail, capture);
			exit(1);
		}

		// This *must* be done in here, to avoid a potential race condition where we no longer own the slot by the time we're assigning
		thread->queue[head] = task;
		new_capture = (new_head << 32) | tail;
	} while (!atomic_compare_exchange_weak(&thread->head_and_tail, &capture, new_capture));

	thread->pool->tasks_left++;
	cond_broadcast(&thread->pool->tasks_available);
}

bool tqueue_pop(Thread *thread, TPoolTask *task) {
	uint64_t capture;
	uint64_t new_capture;
	do {
		capture = thread->head_and_tail;

		uint64_t mask = thread->capacity - 1;
		uint64_t head = (capture >> 32) & mask;
		uint64_t tail = ((uint32_t)capture) & mask;

		uint64_t new_tail = (tail + 1) & mask;
		if (tail == head) {
			return false;
		}

		// Making a copy of the task before we increment the tail, avoiding the same potential race condition as above
		*task = thread->queue[tail];

		new_capture = (head << 32) | new_tail;
	} while (!atomic_compare_exchange_weak(&thread->head_and_tail, &capture, new_capture));

	return true;
}

/*
void sleep_until_work(TPool *pool) {
	syscall(SYS_FUTEX, &pool->tasks_left, FUTEX_WAKE, 
}
*/

void *tpool_worker(void *ptr) {
	TPoolTask task;
	current_thread = (Thread *)ptr;
	TPool *pool = current_thread->pool;
	spall_auto_thread_init(current_thread->idx, SPALL_DEFAULT_BUFFER_SIZE, SPALL_DEFAULT_SYMBOL_CACHE_SIZE);

	for (;;) {
work_start:
		if (!pool->running) {
			break;
		}

		// If we've got tasks to process, work through them
		size_t finished_tasks = 0;
		while (tqueue_pop(current_thread, &task)) {
			task.do_work(task.args);
			pool->tasks_left--;
			finished_tasks += 1;
		}
		if (finished_tasks > 0 && !pool->tasks_left) {
			cond_broadcast(&pool->tasks_available);
		}

		// If there's still work somewhere and we don't have it, steal it
		if (pool->tasks_left) {
			int idx = current_thread->idx;
			for (int i = 0; i < pool->thread_count; i++) {
				if (!pool->tasks_left) {
					break;
				}

				idx = (idx + 1) % pool->thread_count;
				Thread *thread = &pool->threads[idx];

				TPoolTask task;
				if (!tqueue_pop(thread, &task)) {
					continue;
				}

				task.do_work(task.args);
				pool->tasks_left--;

				if (!pool->tasks_left) {
					cond_broadcast(&pool->tasks_available);
				}

				goto work_start;
			}
		}

		// if we've done all our work, and there's nothing to steal, go to sleep
		mutex_lock(&pool->task_lock);
		int ret = cond_wait(&pool->tasks_available, &pool->task_lock);
		if (!ret) {
			mutex_unlock(&pool->task_lock);
		}
	}

	spall_auto_thread_quit();
	return NULL;
}

void tpool_wait(TPool *pool) {
	TPoolTask task;

	while (pool->tasks_left) {

		// if we've got tasks on our queue, run them
		while (tqueue_pop(current_thread, &task)) {
			task.do_work(task.args);
			pool->tasks_left--;
		}

		if (!pool->tasks_left) {
			break;
		}

		// if we've done all our work, and there's nothing to steal, go to sleep
		mutex_lock(&pool->task_lock);
		int ret = cond_wait(&pool->tasks_available, &pool->task_lock);
		if (!ret) {
			mutex_unlock(&pool->task_lock);
		}
	}
}

void thread_start(Thread *thread) {
	pthread_create(&thread->thread, NULL, tpool_worker, (void *)thread);
}
void thread_end(Thread thread) {
	pthread_join(thread.thread, NULL);
	free(thread.queue);
}

void thread_init(TPool *pool, Thread *thread, int idx) {
	thread->capacity = 1 << 14; // must be a power of 2

	thread->queue = calloc(sizeof(TPoolTask), thread->capacity);
	thread->head_and_tail = 0;
	thread->pool = pool;
	thread->idx = idx;
}

TPool *tpool_init(int child_thread_count) {
	TPool *pool = malloc(sizeof(TPool));

	int thread_count = child_thread_count + 1;

	pool->thread_count = thread_count;
	pool->threads = malloc(sizeof(Thread) * pool->thread_count);
	cond_init(&pool->tasks_available);
	mutex_init(&pool->task_lock);
	pool->running = true;

	// setup the main thread
	thread_init(pool, &pool->threads[0], 0);
	current_thread = &pool->threads[0];

	for (int i = 1; i < pool->thread_count; i++) {
		thread_init(pool, &pool->threads[i], i);
		thread_start(&pool->threads[i]);
	}

	return pool;
}

void tpool_destroy(TPool *pool) {
	pool->running = false;
	for (int i = 1; i < pool->thread_count; i++) {
		Thread *thread = &pool->threads[i];
		cond_broadcast(&pool->tasks_available);
		thread_end(pool->threads[i]);
	}

	free(pool->threads[0].queue);
	free(pool->threads);
	free(pool);
}

_Atomic static int total_tasks = 0;
ssize_t little_work(void *args) {

	// this is my workload. enjoy
	int sleep_time = rand() % 301;
	usleep(sleep_time);

	if (total_tasks < 2000) {
		for (int i = 0; i < 5; i++) {
			TPoolTask task;
			task.do_work = little_work;
			task.args = NULL;
			tqueue_push(current_thread, task);
		}
	}

	total_tasks++;
	return 0;
}


int main(void) {
	srand(1);
	spall_auto_init("pool_test.spall");
	spall_auto_thread_init(0, SPALL_DEFAULT_BUFFER_SIZE, SPALL_DEFAULT_SYMBOL_CACHE_SIZE);

	TPool *pool = tpool_init(12);

	int initial_task_count = 10;

	for (int i = 0; i < initial_task_count; i++) {
		TPoolTask task;
		task.do_work = little_work;
		task.args = NULL;
		tqueue_push(current_thread, task);
	}

	tpool_wait(pool);

	total_tasks = 0;
	for (int i = 0; i < initial_task_count; i++) {
		TPoolTask task;
		task.do_work = little_work;
		task.args = NULL;
		tqueue_push(current_thread, task);
	}

	tpool_wait(pool);
	tpool_destroy(pool);

	spall_auto_thread_quit();
	spall_auto_quit();
}

#define SPALL_AUTO_IMPLEMENTATION
#define SPALL_BUFFER_PROFILING
#define SPALL_BUFFER_PROFILING_GET_TIME() __rdtsc()
#include "spall_auto.h"
