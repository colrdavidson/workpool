#include "spall_auto.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#if defined(__linux__) || defined(__APPLE__)

#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>

typedef pthread_mutex_t Mutex;
typedef pthread_t ThreadHandle;
typedef pthread_cond_t CondVar;

#define mutex_init(mut) pthread_mutex_init(mut, NULL)
#define mutex_lock(mut) pthread_mutex_lock(mut)
#define mutex_unlock(mut) pthread_mutex_unlock(mut)

#define thread_start(t) pthread_create(&(t)->thread, NULL, tpool_worker, (void *) (t))
#define thread_end(t)   pthread_join((t)->thread, NULL)

#define cond_init(cond) pthread_cond_init(cond, NULL)
#define cond_broadcast(cond) pthread_cond_broadcast(cond)
#define cond_signal(cond) pthread_cond_signal(cond)
#define cond_wait(cond, mutex) pthread_cond_wait(cond, mutex)

#elif defined(_WIN32) && defined(__clang__)

#include <stdatomic.h>

#elif defined(_WIN32) && !defined(__clang__)
#define _Atomic volatile
#define _Thread_local __declspec(thread)
#endif

#if !defined(__clang__)
#define CAS32(addr, expected, desired) (InterlockedCompareExchange(addr, desired, expected) == expected)
#define CAS64(addr, expected, desired) (InterlockedCompareExchange64(addr, desired, expected) == expected)

#define ATOMIC_INC16(val) (_InterlockedIncrement16(&(val)))
#define ATOMIC_INC32(val) (_InterlockedIncrement(&(val)))
#define ATOMIC_INC64(val) (_InterlockedIncrement64(&(val)))
#define ATOMIC_FUTEX_INC(val) (_InterlockedIncrement64(&(val)))

#define ATOMIC_DEC16(val) (_InterlockedDecrement16(&(val)))
#define ATOMIC_DEC32(val) (_InterlockedDecrement(&(val)))
#define ATOMIC_DEC64(val) (_InterlockedDecrement64(&(val)))
#define ATOMIC_FUTEX_DEC(val) (_InterlockedDecrement64(&(val)))
#else
#define CAS32(addr, expected, desired) atomic_compare_exchange_weak(addr, &expected, desired)
#define CAS64(addr, expected, desired) atomic_compare_exchange_weak(addr, &expected, desired)

#define ATOMIC_INC16(val) ((val)++)
#define ATOMIC_INC32(val) ((val)++)
#define ATOMIC_INC64(val) ((val)++)
#define ATOMIC_FUTEX_INC(val) ((val)++)

#define ATOMIC_DEC16(val) ((val)--)
#define ATOMIC_DEC32(val) ((val)--)
#define ATOMIC_DEC64(val) ((val)--)
#define ATOMIC_FUTEX_DEC(val) ((val)--)
#define __debugbreak() __builtin_trap()
#endif

#if defined(__linux__)

#include <linux/futex.h>
#include <sys/syscall.h>

typedef _Atomic int32_t Futex;

#elif defined(__APPLE__)

typedef _Atomic int64_t Futex;

#elif defined(_WIN32)

#include <windows.h>
#include <process.h>

typedef ptrdiff_t ssize_t;
typedef CRITICAL_SECTION Mutex;
typedef HANDLE ThreadHandle;
typedef CONDITION_VARIABLE CondVar;

#define mutex_init(mut) InitializeCriticalSection(mut)
#define mutex_lock(mut) EnterCriticalSection(mut)
#define mutex_unlock(mut) LeaveCriticalSection(mut)

#define thread_start(t) ((t)->thread = (HANDLE) _beginthread(tpool_worker, 0, t))
#define thread_end(t) WaitForSingleObject((t)->thread, INFINITE)

#define cond_init(cond) InitializeConditionVariable(cond)
#define cond_broadcast(cond) WakeAllConditionVariable(cond)
#define cond_signal(cond) WakeConditionVariable(cond)
#define cond_wait(cond, mutex) SleepConditionVariableCS(cond, mutex, INFINITE)

typedef _Atomic int64_t Futex;
#endif

typedef void tpool_task_proc(void *data);

typedef struct TPoolTask {
	tpool_task_proc  *do_work;
	void             *args;
} TPoolTask;

typedef struct Thread {
	ThreadHandle thread;
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

	CondVar tasks_available;
	Mutex task_lock;

	Futex tasks_left;
} TPool;

_Thread_local Thread *current_thread = NULL;
_Thread_local int work_count = 0;

#if defined(__linux__)
void tpool_wake_addr(Futex *addr) {
	for (;;) {
		int ret = syscall(SYS_futex, addr, FUTEX_WAKE, 1, NULL, NULL, 0);
		if (ret == -1) {
			perror("Futex wake");
			__debugbreak();
		} else if (ret > 0) {
			return;
		}
	}
}

void tpool_wait_on_addr(Futex *addr, Futex val) {
	for (;;) {
		int ret = syscall(SYS_futex, addr, FUTEX_WAIT, val, NULL, NULL, 0);
		if (ret == -1) {
			if (errno != EAGAIN) {
				perror("Futex wait");
				__debugbreak();
			} else {
				return;
			}
		} else if (ret == 0) {
			if (*addr != val) {
				return;
			}
		}
	}
}
#elif defined(__APPLE__)

#define UL_COMPARE_AND_WAIT	0x00000001
#define ULF_NO_ERRNO        0x01000000

int __ulock_wait(uint32_t operation, void *addr, uint64_t value, uint32_t timeout); /* timeout is specified in microseconds */
int __ulock_wake(uint32_t operation, void *addr, uint64_t wake_value);

void tpool_wake_addr(Futex *addr) {
	for (;;) {
		int ret = __ulock_wake(UL_COMPARE_AND_WAIT | ULF_NO_ERRNO, addr, 0);
		if (ret >= 0) {
			return;
		}
		if (ret == EINTR || ret == EFAULT) {
			continue;
		}
		if (ret == ENOENT) {
			return;
		}
		printf("futex wake fail?\n");
		__debugbreak();
	}
}

void tpool_wait_on_addr(Futex *addr, Futex val) {
	for (;;) {
		int ret = __ulock_wait(UL_COMPARE_AND_WAIT | ULF_NO_ERRNO, addr, val, 0);
		if (ret >= 0) {
			if (*addr != val) {
				return;
			}
			continue;
		}
		if (ret == EINTR || ret == EFAULT) {
			continue;
		}
		if (ret == ENOENT) {
			return;
		}

		printf("futex wait fail?\n");
		__debugbreak();
	}
}
#elif defined(_WIN32)
void tpool_wake_addr(Futex *addr) {
	WakeByAddressSingle((void *)addr);
}

void tpool_wait_on_addr(Futex *addr, Futex val) {
	for (;;) {
		int ret = WaitOnAddress(addr, (void *)&val, sizeof(val), INFINITE);
		if (*addr != val) break;
	}
}
#endif

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
			__debugbreak();
		}

		// This *must* be done in here, to avoid a potential race condition where we no longer own the slot by the time we're assigning
		thread->queue[head] = task;
		new_capture = (new_head << 32) | tail;
	} while (!CAS64(&thread->head_and_tail, capture, new_capture));

	ATOMIC_FUTEX_INC(thread->pool->tasks_left);
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
	} while (!CAS64(&thread->head_and_tail, capture, new_capture));

	return true;
}


#ifndef _WIN32
void *tpool_worker(void *ptr)
#else
void tpool_worker(void *ptr)
#endif
{
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
			ATOMIC_FUTEX_DEC(pool->tasks_left);

			finished_tasks += 1;
		}
		if (finished_tasks > 0 && !pool->tasks_left) {
			tpool_wake_addr(&pool->tasks_left);
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
				ATOMIC_FUTEX_DEC(pool->tasks_left);

				if (!pool->tasks_left) {
					tpool_wake_addr(&pool->tasks_left);
				}

				goto work_start;
			}
		}

		// if we've done all our work, and there's nothing to steal, go to sleep
		mutex_lock(&pool->task_lock);
		int ret = cond_wait(&pool->tasks_available, &pool->task_lock);
		// if (!ret) {
		mutex_unlock(&pool->task_lock);
		// }
	}

	spall_auto_thread_quit();
#ifndef _WIN32
	return NULL;
#endif
}

void tpool_wait(TPool *pool) {
	TPoolTask task;

	while (pool->tasks_left) {

		// if we've got tasks on our queue, run them
		while (tqueue_pop(current_thread, &task)) {
			task.do_work(task.args);
			ATOMIC_FUTEX_DEC(pool->tasks_left);
		}


		// is this mem-barriered enough?
		// This *must* be executed in this order, so the futex wakes immediately
		// if rem_tasks has changed since we checked last, otherwise the program
		// will permanently sleep
		Futex rem_tasks = pool->tasks_left;
		if (!rem_tasks) {
			break;
		}

		tpool_wait_on_addr(&pool->tasks_left, rem_tasks);
	}
}

void thread_init(TPool *pool, Thread *thread, int idx) {
	thread->capacity = 1 << 14; // must be a power of 2

	thread->queue = calloc(sizeof(TPoolTask), thread->capacity);
	thread->head_and_tail = 0;
	thread->pool = pool;
	thread->idx = idx;
}

void tpool_init(TPool *pool, int child_thread_count) {
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
}

void tpool_destroy(TPool *pool) {
	pool->running = false;
	for (int i = 1; i < pool->thread_count; i++) {
		Thread *thread = &pool->threads[i];
		cond_broadcast(&pool->tasks_available);
		thread_end(&pool->threads[i]);
	}
	for (int i = 0; i < pool->thread_count; i++) {
		free(pool->threads[i].queue);
	}

	free(pool->threads);
}

static float aaa[10000];

_Atomic static int total_tasks = 0;
void little_work(void *args) {
	// this is my workload. enjoy

#ifndef _WIN32
	int sleep_time = rand() % 201;
	usleep(sleep_time);
#else
	for (size_t i = 0; i < 10000; i++) {
		aaa[i] = (rand() % 2000) * 0.25;
	}
#endif

	if (total_tasks < 2000) {
		for (int i = 0; i < 5; i++) {
			TPoolTask task;
			task.do_work = little_work;
			task.args = NULL;
			tqueue_push(current_thread, task);
		}
	}

	ATOMIC_INC32(total_tasks);
}


int main(void) {
	srand(1);
	spall_auto_init("pool_test.spall");
	spall_auto_thread_init(0, SPALL_DEFAULT_BUFFER_SIZE, SPALL_DEFAULT_SYMBOL_CACHE_SIZE);

	TPool pool = {};
	tpool_init(&pool, 32);

	int initial_task_count = 10;

	for (int i = 0; i < initial_task_count; i++) {
		TPoolTask task;
		task.do_work = little_work;
		task.args = NULL;
		tqueue_push(current_thread, task);
	}

	tpool_wait(&pool);

	total_tasks = 0;
	for (int i = 0; i < initial_task_count; i++) {
		TPoolTask task;
		task.do_work = little_work;
		task.args = NULL;
		tqueue_push(current_thread, task);
	}

	tpool_wait(&pool);

	total_tasks = 0;
	for (int i = 0; i < initial_task_count; i++) {
		TPoolTask task;
		task.do_work = little_work;
		task.args = NULL;
		tqueue_push(current_thread, task);
	}
	tpool_wait(&pool);
	tpool_destroy(&pool);

	spall_auto_thread_quit();
	spall_auto_quit();
}

#define SPALL_AUTO_IMPLEMENTATION
#include "spall_auto.h"
