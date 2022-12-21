#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

typedef ssize_t tpool_task_proc(void *data);
typedef struct TPoolTask {
	struct TPoolTask *next;
	tpool_task_proc  *do_work;
	void             *args;
} TPoolTask;

typedef struct Thread {
	pthread_t thread;
	int idx;

	TPoolTask *queue;
	pthread_mutex_t queue_lock;
	_Atomic int64_t queue_len;

	struct TPool *pool;
} Thread;

typedef struct TPool {
	struct Thread *threads;

	int thread_count;
	bool running;

	// if these wrap, you're doing it wrong.
	_Atomic uint64_t tasks_done;
	_Atomic uint64_t tasks_total;
} TPool;

_Thread_local Thread *current_thread = NULL;
_Thread_local int work_count = 0;

void tqueue_push(Thread *thread, TPoolTask *task) {
	thread->queue_len += 1;
	thread->pool->tasks_total++;

	printf("pushing task onto thread %d\n", thread->idx);
	if (!thread->queue) {
		thread->queue = task;
		return;
	}

	TPoolTask *old_head = thread->queue;
	task->next = old_head;
	thread->queue = task;
}

void tqueue_push_safe(Thread *thread, TPoolTask *task) {
	pthread_mutex_lock(&current_thread->queue_lock);
	tqueue_push(thread, task);
	pthread_mutex_unlock(&current_thread->queue_lock);
}

TPoolTask *tqueue_pop(Thread *thread) {
	if (thread->queue) {
		TPoolTask *task = thread->queue;
		thread->queue = task->next;

		thread->queue_len -= 1;
		return task;
	}

	return NULL;
}

TPoolTask *tqueue_pop_safe(Thread *thread) {
	pthread_mutex_lock(&current_thread->queue_lock);
	TPoolTask *task = tqueue_pop(thread);
	pthread_mutex_unlock(&current_thread->queue_lock);
	return task;
}

void *tpool_worker(void *ptr) {
	current_thread = (Thread *)ptr;

	for (;;) {
		if (!current_thread->pool->running) {
			break;
		}

		TPoolTask *task = tqueue_pop_safe(current_thread);
		if (!task) {
			continue;
		}

		task->do_work(task->args);
		current_thread->pool->tasks_done++;
	}

	return NULL;
}

void thread_init(TPool *pool, Thread *thread, int idx) {
	printf("creating thread %d\n", idx);
	pthread_mutex_init(&thread->queue_lock, NULL);
	thread->queue = NULL;
	thread->pool = pool;
	thread->idx = idx;
}

TPool *tpool_init(int child_thread_count) {
	TPool *pool = malloc(sizeof(TPool));

	int thread_count = child_thread_count + 1;

	pool->thread_count = thread_count;
	pool->threads = malloc(sizeof(Thread) * pool->thread_count);
	pool->running = true;

	// setup the main thread
	thread_init(pool, &pool->threads[0], 0);
	current_thread = &pool->threads[0];

	for (int i = 1; i < pool->thread_count; i++) {
		thread_init(pool, &pool->threads[i], i);
		pthread_create(&pool->threads[i].thread, NULL, tpool_worker, (void *)&pool->threads[i]);
	}

	return pool;
}

void tpool_destroy(TPool *pool) {
	pool->running = false;
	for (int i = 1; i < pool->thread_count - 1; i++) {
		pthread_join(pool->threads[i].thread, NULL);
	}
	free(pool->threads);
	free(pool);
}

ssize_t little_work(void *args) {
	size_t count = (size_t)args;
	printf("%d -- %lu -- %zu\n", current_thread->idx, pthread_self(), count);

	if (current_thread->pool->tasks_total < 20) {
		TPool *pool = current_thread->pool;
		int64_t sibling_idx = (current_thread->idx + 1) % pool->thread_count;
		Thread *sibling_thread = &pool->threads[sibling_idx];

		TPoolTask *task = malloc(sizeof(TPoolTask));
		task->do_work = little_work;
		task->args = (void *)(uint64_t)(count);
		
		tqueue_push_safe(sibling_thread, task);
	}
	return 0;
}

void tpool_wait(TPool *pool) {
	while (pool->tasks_done < pool->tasks_total) {
		while (current_thread->queue_len > 0) {
			TPoolTask *task = tqueue_pop(current_thread);
			if (!task) {
				break;
			}

			task->do_work(task->args);
			pool->tasks_done++;
		}
	}
}

int main(void) {
	TPool *pool = tpool_init(1);

	int initial_task_count = 10;
	TPoolTask *tasks = malloc(sizeof(TPoolTask) * initial_task_count);

	pthread_mutex_lock(&current_thread->queue_lock);
	for (int i = 0; i < initial_task_count; i++) {
		tasks[i].do_work = little_work;
		tasks[i].args = (void *)(uint64_t)(i + 1);
		tqueue_push(current_thread, &tasks[i]);
	}
	pthread_mutex_unlock(&current_thread->queue_lock);

	tpool_wait(pool);
	printf("%lu, %lu\n", pool->tasks_done, pool->tasks_total);
	tpool_destroy(pool);
}
