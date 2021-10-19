#ifndef THREADPOOL_H_
#define THREADPOOL_H_
//Followed the tutorial on https://nachtimwald.com/2019/04/12/thread-pool-in-c/
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>

typedef void (*thread_func_t)(void *arg);
typedef int (comparator)(void*, void*);
typedef bool (predicate)(void*);
typedef void fucntor(void*);


typedef struct tworker_t{
    thread_func_t job;
    void* arg;
    struct tworker_t* next;
} tworker_t;

typedef struct  {
    pthread_mutex_t mutex;
    size_t working_cnt;
    size_t thread_cnt;
    pthread_cond_t no_threads;
    pthread_cond_t avail_job;
    tworker_t* head;
    tworker_t* tail;
    bool stop;
} tpool_t ;

tpool_t *tpool_create(size_t num);
void tpool_destroy(tpool_t *tm);

bool tpool_add_work(tpool_t *tm, thread_func_t func, void *arg);
void tpool_wait(tpool_t *tm);
void dp_quick_sort(void* arr, size_t size,size_t n,comparator comp);

//Synchronous array functions
void tpool_sort(tpool_t* pool, void* arr, size_t n,size_t size,comparator compare);
void tpool_forEach(tpool_t* tm, void* arr, size_t size);
void tpool_filter(tpool_t* tm, void* arr, size_t size);
void merge(void* arr, size_t size,size_t low, size_t mid, size_t high, comparator compare);

/*
TODO:
1)FIND IMPROVEMENTS
2)Parr Array Functions
3)Ensure security
4)HaskellC
*/
#endif
// SOURCE : https://blog.teamleadnet.com/2014/05/java-8-parallel-sort-internals.html
// Parallel sort will be used the same way as Java implements their pararell sort
// if the array size is small enough (<= 8192 elements) or the number of reported logical CPUs is 1

/*
    he algorithm (Arrays.java) will decide on the chunk size before submitting to sort the array using the helper (ArraysParallelSortHelpers.java). The helper also receives a working buffer as this parallel sort implementation will require the same amount of memory as the original array. This property itself may make it unsuitable for certain cases, like almost sorted arrays or extremely large arrays.

    The chunk size that will be distributed among CPUs will be at least 8192 items, or Array size / (4 * reported CPUs) if the latter is larger.
    So, an array of 500.000 items with a standard 2 core, 4 virtual cores machine would have:

    500.000 / (4 * 4) = 31.250 items / chunk

    The original array is split into these 4 logical blocks, which are sorted then merged together. With the 4 logical processing units, each quarter is split into another quarter (16th), then sorted and merged back to the original array.

    The sorting itself is always the dual pivot quick sort for raw types (int, byte, short…) and TimSort for anything else.
*/



