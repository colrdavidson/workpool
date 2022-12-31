#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

// cross-platform thread wrappers, because microsoft couldn't be arsed to take 5 seconds and 
// do this and save all the junior devs and codebases everywhere from this pile of nonsense.
#if defined(__linux__) || defined(__APPLE__)

#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>

typedef pthread_mutex_t TPool_Mutex;
typedef pthread_t TPool_ThreadHandle;
typedef pthread_cond_t TPool_CondVar;

#define tpool_mutex_init(mut) pthread_mutex_init(mut, NULL)
#define tpool_mutex_lock(mut) pthread_mutex_lock(mut)
#define tpool_mutex_unlock(mut) pthread_mutex_unlock(mut)

#define tpool_thread_start(t) pthread_create(&(t)->thread, NULL, _tpool_worker, (void *) (t))
#define tpool_thread_end(t)   pthread_join((t)->thread, NULL)

#define tpool_cond_init(cond) pthread_cond_init(cond, NULL)
#define tpool_cond_broadcast(cond) pthread_cond_broadcast(cond)
#define tpool_cond_signal(cond) pthread_cond_signal(cond)
#define tpool_cond_wait(cond, mutex) pthread_cond_wait(cond, mutex)

#elif defined(_WIN32)

#include <windows.h>
#include <process.h>

typedef ptrdiff_t ssize_t;
typedef CRITICAL_SECTION TPool_Mutex;
typedef HANDLE TPool_ThreadHandle;
typedef CONDITION_VARIABLE TPool_CondVar;

#define tpool_mutex_init(mut) InitializeCriticalSection(mut)
#define tpool_mutex_lock(mut) EnterCriticalSection(mut)
#define tpool_mutex_unlock(mut) LeaveCriticalSection(mut)

#define tpool_thread_start(t) ((t)->thread = (HANDLE) _beginthread(_tpool_worker, 0, t))
#define tpool_thread_end(t) WaitForSingleObject((t)->thread, INFINITE)

#define tpool_cond_init(cond) InitializeConditionVariable(cond)
#define tpool_cond_broadcast(cond) WakeAllConditionVariable(cond)
#define tpool_cond_signal(cond) WakeConditionVariable(cond)
#define tpool_cond_wait(cond, mutex) SleepConditionVariableCS(cond, mutex, INFINITE)

#endif

// MSVC only took 11 years to put C11 atomics in, (despite the fact that MSVC/C++11 has them).
// This is the pain we suffer because microsoft got lazy
#if !defined(__clang__)

#define TPool_Thread_Local __declspec(thread)
#define TPool_Atomic volatile

#define TPOOL_CAS(addr, expected, desired) (InterlockedCompareExchange64(addr, desired, expected) == expected)
#define TPOOL_ATOMIC_FUTEX_INC(val) (_InterlockedIncrement64(&(val)))
#define TPOOL_ATOMIC_FUTEX_DEC(val) (_InterlockedDecrement64(&(val)))

#else

#include <stdatomic.h>

#define TPool_Thread_Local _Thread_local
#define TPool_Atomic _Atomic

#define TPOOL_CAS(addr, expected, desired) atomic_compare_exchange_weak(addr, &expected, desired)
#define TPOOL_ATOMIC_FUTEX_INC(val) ((val)++)
#define TPOOL_ATOMIC_FUTEX_DEC(val) ((val)--)
#define __debugbreak() __builtin_trap()

#endif

// cross-platform futex, because we can't just have nice things. All the popular platforms have them under the hood,
// but giving them to users? NO! Users are too stupid to have nice things, save them for the fedora-wearing elite.
#if defined(__linux__)

#include <linux/futex.h>
#include <sys/syscall.h>

typedef TPool_Atomic int32_t TPool_Futex;

void _tpool_wake_addr(TPool_Futex *addr) {
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

void _tpool_wait_on_addr(TPool_Futex *addr, TPool_Futex val) {
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

typedef TPool_Atomic int64_t TPool_Futex;

#define UL_COMPARE_AND_WAIT	0x00000001
#define ULF_NO_ERRNO        0x01000000

/* timeout is specified in microseconds */
int __ulock_wait(uint32_t operation, void *addr, uint64_t value, uint32_t timeout); 
int __ulock_wake(uint32_t operation, void *addr, uint64_t wake_value);

void _tpool_wake_addr(TPool_Futex *addr) {
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

void _tpool_wait_on_addr(TPool_Futex *addr, TPool_Futex val) {
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
typedef TPool_Atomic int64_t TPool_Futex;

void _tpool_wake_addr(TPool_Futex *addr) {
	WakeByAddressSingle((void *)addr);
}

void _tpool_wait_on_addr(TPool_Futex *addr, TPool_Futex val) {
	for (;;) {
		int ret = WaitOnAddress(addr, (void *)&val, sizeof(val), INFINITE);
		if (*addr != val) break;
	}
}
#endif

struct TPool;
typedef void tpool_task_proc(struct TPool *pool, void *data);
TPool_Thread_Local int tpool_current_thread_idx;

typedef struct TPool_Task {
	tpool_task_proc  *do_work;
	void             *args;
} TPool_Task;

typedef struct TPool_Thread {
	TPool_ThreadHandle thread;
	int idx;

	TPool_Task *queue;
	size_t capacity;
	TPool_Atomic uint64_t head_and_tail;

	struct TPool *pool;
} TPool_Thread;

typedef struct TPool {
	struct TPool_Thread *threads;

	int thread_count;
	TPool_Atomic bool running;

	TPool_CondVar tasks_available;
	TPool_Mutex task_lock;

	TPool_Futex tasks_left;
} TPool;

void _thread_init(TPool *pool, TPool_Thread *thread, int idx) {
	thread->capacity = 1 << 14; // must be a power of 2

	thread->queue = calloc(sizeof(TPool_Task), thread->capacity);
	thread->head_and_tail = 0;
	thread->pool = pool;
	thread->idx = idx;
}

void _tpool_queue_push(TPool_Thread *thread, TPool_Task task) {
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

		// This *must* be done in here, to avoid a potential race condition where we
		// no longer own the slot by the time we're assigning
		thread->queue[head] = task;
		new_capture = (new_head << 32) | tail;
	} while (!TPOOL_CAS(&thread->head_and_tail, capture, new_capture));

	TPOOL_ATOMIC_FUTEX_INC(thread->pool->tasks_left);
	tpool_cond_broadcast(&thread->pool->tasks_available);
}

bool _tpool_queue_pop(TPool_Thread *thread, TPool_Task *task) {
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

		// Making a copy of the task before we increment the tail, 
		// avoiding the same potential race condition as above
		*task = thread->queue[tail];

		new_capture = (head << 32) | new_tail;
	} while (!TPOOL_CAS(&thread->head_and_tail, capture, new_capture));

	return true;
}

#ifndef _WIN32
void *_tpool_worker(void *ptr)
#else
void _tpool_worker(void *ptr)
#endif
{
	TPool_Task task;

	TPool_Thread *current_thread = (TPool_Thread *)ptr;
	int tpool_current_thread_idx = current_thread->idx;
	TPool *pool = current_thread->pool;

	for (;;) {
        work_start:
		if (!pool->running) {
			break;
		}

		// If we've got tasks to process, work through them
		size_t finished_tasks = 0;
		while (_tpool_queue_pop(current_thread, &task)) {
			task.do_work(pool, task.args);
			TPOOL_ATOMIC_FUTEX_DEC(pool->tasks_left);

			finished_tasks += 1;
		}
		if (finished_tasks > 0 && !pool->tasks_left) {
			_tpool_wake_addr(&pool->tasks_left);
		}

		// If there's still work somewhere and we don't have it, steal it
		if (pool->tasks_left) {
			int idx = current_thread->idx;
			for (int i = 0; i < pool->thread_count; i++) {
				if (!pool->tasks_left) {
					break;
				}

				idx = (idx + 1) % pool->thread_count;
				TPool_Thread *thread = &pool->threads[idx];

				TPool_Task task;
				if (!_tpool_queue_pop(thread, &task)) {
					continue;
				}

				task.do_work(pool, task.args);
				TPOOL_ATOMIC_FUTEX_DEC(pool->tasks_left);

				if (!pool->tasks_left) {
					_tpool_wake_addr(&pool->tasks_left);
				}

				goto work_start;
			}
		}

		// if we've done all our work, and there's nothing to steal, go to sleep
		tpool_mutex_lock(&pool->task_lock);
		int ret = tpool_cond_wait(&pool->tasks_available, &pool->task_lock);
		tpool_mutex_unlock(&pool->task_lock);
	}

#ifndef _WIN32
	return NULL;
#endif
}

void tpool_add_task(TPool *pool, TPool_Task task) {
	TPool_Thread *current_thread = &pool->threads[tpool_current_thread_idx];
	_tpool_queue_push(current_thread, task);
}

void tpool_wait(TPool *pool) {
	TPool_Task task;
	TPool_Thread *current_thread = &pool->threads[tpool_current_thread_idx];

	while (pool->tasks_left) {

		// if we've got tasks on our queue, run them
		while (_tpool_queue_pop(current_thread, &task)) {
			task.do_work(pool, task.args);
			TPOOL_ATOMIC_FUTEX_DEC(pool->tasks_left);
		}


		// is this mem-barriered enough?
		// This *must* be executed in this order, so the futex wakes immediately
		// if rem_tasks has changed since we checked last, otherwise the program
		// will permanently sleep
		TPool_Futex rem_tasks = pool->tasks_left;
		if (!rem_tasks) {
			break;
		}

		_tpool_wait_on_addr(&pool->tasks_left, rem_tasks);
	}
}

void tpool_init(TPool *pool, int child_thread_count) {
	int thread_count = child_thread_count + 1;
	pool->thread_count = thread_count;
	pool->threads = malloc(sizeof(TPool_Thread) * pool->thread_count);

	tpool_cond_init(&pool->tasks_available);
	tpool_mutex_init(&pool->task_lock);
	pool->running = true;

	// setup the main thread
	_thread_init(pool, &pool->threads[0], 0);
	tpool_current_thread_idx = 0;

	for (int i = 1; i < pool->thread_count; i++) {
		_thread_init(pool, &pool->threads[i], i);
		tpool_thread_start(&pool->threads[i]);
	}
}

void tpool_destroy(TPool *pool) {
	pool->running = false;
	for (int i = 1; i < pool->thread_count; i++) {
		TPool_Thread *thread = &pool->threads[i];
		tpool_cond_broadcast(&pool->tasks_available);
		tpool_thread_end(&pool->threads[i]);
	}
	for (int i = 0; i < pool->thread_count; i++) {
		free(pool->threads[i].queue);
	}

	free(pool->threads);
}
