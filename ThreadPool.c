#include "ThreadPool.h"

#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

//from the gcc statements and expressions
#define maxsint(a,b) \
  ({size_t _a = (a), _b = (b); _a > _b ? _a : _b; })
#define minsint(a,b) \
  ({size_t _a = (a), _b = (b); _a < _b ? _a : _b; })

#ifdef _WIN32
#include <windows.h>
#include <math.h>
#elif MACOS
#include <sys/param.h>
#include <sys/sysctl.h>
#else
#include <unistd.h>
#include <math.h>
#endif

//http://www.cprogramming.com/snippets/source-code/find-the-number-of-cpu-cores-for-windows-mac-or-linux
//allows function to work on win/linux/MacOS
int num_of_cores() {
#ifdef WIN32
    SYSTEM_INFO sysinfo;
    GetSystemInfo(&sysinfo);
    return sysinfo.dwNumberOfProcessors;
#elif MACOS
    int nm[2];
    size_t len = 4;
    uint32_t count;
 
    nm[0] = CTL_HW; nm[1] = HW_AVAILCPU;
    sysctl(nm, 2, &count, &len, NULL, 0);
 
    if(count < 1) {
    nm[1] = HW_NCPU;
    sysctl(nm, 2, &count, &len, NULL, 0);
    if(count < 1) { count = 1; }
    }
    return count;
#else
    return sysconf(_SC_NPROCESSORS_ONLN);
#endif
}

#ifndef MAX_NUM_THREADS
#define MAX_NUM_THREADS num_of_cores() + 1
#endif



static tworker_t* create_worker_t(thread_func_t func, void* arg){
    tworker_t* work;
    if(func == NULL) return NULL;

    work = (tworker_t*)malloc(sizeof(tworker_t));
    work->job = func;
    work->arg = arg;
    work->next = NULL;
    return work;
}

static void destroy_worker_t(tworker_t* work) {
    if(work == NULL) return ;

    free(work);
}

static tworker_t* get_worker_t(tpool_t* pool) {
    if(pool ==  NULL || pool->head == NULL)
        return NULL;
    tworker_t* ret = pool->head;
    if(ret->next == NULL) {
        pool->head = NULL;
        pool->tail = NULL;
    }else {
        pool->head = ret->next;
    }
    return ret;
}

bool tpool_add_work(tpool_t *tm, thread_func_t func, void *arg) {
    if(tm == NULL) {
        return false;
    }
    tworker_t* work = create_worker_t(func,arg);
    if(work == NULL) return false;

    pthread_mutex_lock(&(tm->mutex));
    if (tm->head == NULL) {
        tm->head = work;
        tm->tail  = work;
    } else {
        tm->tail->next = work;
        tm->tail       = work;
    }

    pthread_cond_broadcast(&(tm->avail_job));
    pthread_mutex_unlock(&(tm->mutex));
    return true;
}

static void* tpool_worker(void* arg_pool) // Links thread with the threadpool
{
     tpool_t* pool = (tpool_t*) arg_pool;//thread_pool
     tworker_t* worker;//worker
     while(1) {

        pthread_mutex_lock(&pool->mutex);//lock thread and mutex and wait for available job
            while(pool->head == NULL && !pool->stop)
                pthread_cond_wait(&pool->avail_job, &pool->mutex);//wait for available job 
            
            if(pool->stop)//if pool signaled stop break the loop
                break;
            
            worker = get_worker_t(pool);//get work from get_worker_t
            pool->working_cnt++;
        pthread_mutex_unlock(&pool->mutex);

        
        if(worker != NULL) {
            worker->job(worker->arg);//execute function
            destroy_worker_t(worker);//destroy the worker
        }
        //lock the thread to manipulate worker count and to check if there are other any jobs that are needed to run
        pthread_mutex_lock(&pool->mutex);
            pool->working_cnt--;
            //if pool was not signaled to stop and
            //if there is no more work
            // and if the heap is empty
            // signal that the threads are no longer processing any work
            if(!pool->stop && pool->working_cnt == 0 && pool->head == NULL) pthread_cond_signal(&pool->no_threads);
        pthread_mutex_unlock(&pool->mutex);

     }
     pool->thread_cnt--;//reduce thread counts
     pthread_cond_signal(&pool->no_threads);//in case it did not reach last method, signal again
     pthread_mutex_unlock(&(pool->mutex));//unlock mutex
     return NULL;
}

tpool_t* tpool_create(size_t  num_threads) {//Create a tpool_t type
    tpool_t* ret;
    pthread_t thread;
    size_t n_threads = minsint(num_threads, MAX_NUM_THREADS);//avoid creating more threads than the specific processor can handle
    if(n_threads == 0)
        n_threads = MAX_NUM_THREADS;
    ret = (tpool_t*)calloc(1, sizeof(tpool_t));//create thread pool in heap
    ret->thread_cnt = n_threads;
    pthread_mutex_init(&ret->mutex,NULL);
    pthread_cond_init(&ret->avail_job, NULL);
    pthread_cond_init(&ret->no_threads,NULL);
    ret->head = NULL;
    ret->tail = NULL;
    
   for(size_t i = 0; i < n_threads ;++i ) {
        pthread_create(&thread, NULL, tpool_worker, (void*)ret);//create n_threads and connect them to the thread  pool
        pthread_detach(thread);
    }
    return ret;
}


void tpool_destroy(tpool_t* pool) {
    if(pool == NULL)
        return;
    tworker_t* next_iter;
    tworker_t* iter;


    pthread_mutex_lock(&pool->mutex);
    iter = pool->head;
    while(iter != NULL) {
        next_iter = iter->next;
        destroy_worker_t(iter);
        iter = next_iter;
    }
    pool->stop = true;
    pthread_cond_broadcast(&pool->avail_job);//broadcast to all threads that jobs are available in order for them to break out of the loop
    pthread_mutex_unlock(&pool->mutex);

    tpool_wait(pool);

    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->avail_job);
    pthread_cond_destroy(&pool->no_threads);

    free(pool);
}

void tpool_wait(tpool_t* pool) {//wait for threads to finish their job
    if( pool == NULL ) 
        return;
    pthread_mutex_lock(&pool->mutex);
    while(1) {
        //if the pool hasn't signaled to stop and there is work to be done 
        // OR
        // the pool signaled stop and there are still running threads
        if((!pool->stop && pool->working_cnt != 0) || (pool->stop && pool->thread_cnt != 0)) {
            pthread_cond_wait(&pool->no_threads, &pool->mutex);//wait for all threads to signal that they are finished and release the mutex until they are
         } else {
             break;
         }
    }
    
    pthread_mutex_unlock(&pool->mutex);
}

static void tpool_partial_wait(pthread_mutex_t* p_mutex, pthread_cond_t* p_cond, size_t* counter) {
    pthread_mutex_lock(p_mutex);
    while(1) {
        //if the pool hasn't signaled to stop and there is work to be done 
        // OR
        // the pool signaled stop and there are still running threads
        if(*counter != 0) {
            pthread_cond_wait(p_cond, p_mutex);//wait for all threads to signal that they are finished and release the mutex until they are
         } else {
             break;
         }
    }
    pthread_mutex_unlock(p_mutex);
}

static void swap(void* a, void* b,size_t size) {
    void* temp = malloc(size);
    memcpy(temp,a,size);
    memcpy(a,b,size);
    memcpy(b,temp,size);
    free(temp);
}

static size_t partition(void* arr, size_t size, size_t low, size_t high, size_t* lp, comparator comp) {
    if(comp(arr + size * low ,arr + size *high) > 0)
        swap(arr + size *low,arr + size *high,size);
    size_t j = low + 1;
    size_t g = high - 1;
    size_t k = j;
    void* p = arr + size *low;
    void* q = arr + size *high;
    while(k <= g) {
        //if elements less than left pivot
        if(comp(arr + size *k ,p) < 0){
            swap(arr + size *k,arr + size *j,size);
            j++;
        //elements greater or eqial to the right pivot
        }else if(comp(arr + size * k,q) >= 0 ) {
            while(comp(arr + size * g,q) > 0 && k < g)
                g--;
            swap(arr + size *k,arr + size *g,size);
            g--;
            if(comp(arr + size *k,p) < 0){
                swap(arr + size *k,arr + size *j,size);
                j++;
            }
        }
        k++;
    }
    j--;
    g++;
    swap(arr + size *low, arr + size * j,size);
    swap(arr + size *high, arr + size * g,size);
    *lp = j;

    return g;
}

static void dp_quick_sort_h(void* arr, size_t size, size_t low, size_t high,comparator comp) {
    
    if(low < high) {
        size_t lp,rp;
        
        
        rp = partition(arr,size,low,high,&lp,comp);
        if(lp != 0)//negate overflow
            dp_quick_sort_h(arr,size,low,lp - 1, comp);
        dp_quick_sort_h(arr,size,lp + 1,rp - 1, comp );
        dp_quick_sort_h(arr,size,rp + 1,high, comp   );
    }
}

void merge(void* arr, size_t size,size_t low, size_t mid, size_t high, comparator compare){//in place merge using shell sort
    size_t len_l = (mid - low + 1) ;
    size_t len_h = (high - mid) ;
    void* arr1 = malloc(len_l * size);
    void* arr2 = malloc(len_h * size);

    memcpy(arr1, arr + low * size,len_l * size);
    memcpy(arr2,arr + (low + len_l) * size,len_h * size);
    int i = 0;
    int j = 0;
    int k = low;
    while(i < len_l && j < len_h) {
        if(compare(arr1 + i * size,arr2 + j * size) == 0) {
            memcpy(arr + k * size,arr1 + i * size,size);
            i++;j++;
        }else if(compare(arr1 + i * size,arr2 + j * size) < 0) {
            memcpy(arr + k * size,arr1 + i * size,size);
            i++;
        } else {
            memcpy(arr + k * size,arr2 + j * size,size);
            j++;
        }
        k++;
    }

    while(i < len_l) {
        memcpy(arr + k * size,arr1 + i * size,size);
        k++;i++;
    } 

    while(j < len_h) {
        memcpy(arr + k * size,arr2 + j * size,size);
        k++;j++;
    }
    free(arr1);
    free(arr2);
    // tester1(arr + low * size, high - low + 1);

}

void dp_quick_sort(void* arr, size_t size,size_t n,comparator comp) {
    dp_quick_sort_h(arr,size,0,n - 1,comp);
}

typedef struct {
    pthread_mutex_t* mutex;
    pthread_cond_t cond;
    size_t counter;
    
} pool_wrapper;

typedef struct {
    comparator* compare;
    void* arr;
    size_t size;
    size_t n;
} sort_wrapper;

typedef struct {
    comparator* compare;
    void* arr;
    size_t size;
    size_t low;
    size_t mid;
    size_t high;

} merge_wrapper;


typedef struct {
    pool_wrapper* wrapper;
    thread_func_t func;
    void* arg;
} job;


static void tpool_wrapper(void* arg1){
    job* foo_func = (job*) arg1;
    pool_wrapper* args = foo_func->wrapper;
    
    (*foo_func->func)(foo_func->arg);
    
    pthread_mutex_lock(args->mutex);
    args->counter--;
    pthread_cond_signal(&args->cond);
    pthread_mutex_unlock(args->mutex);
}

static void sort_wrapper_func(void* arg) {
    sort_wrapper* s_arg = (sort_wrapper*) arg;
    dp_quick_sort(s_arg->arr,s_arg->size,s_arg->n,s_arg->compare);

}

static void merge_wrapper_func(void* arg) {
    merge_wrapper* s_arg = (merge_wrapper*) arg;
    merge(s_arg->arr,s_arg->size,s_arg->low,s_arg->mid,s_arg->high,s_arg->compare);
}

//Pararell Sort Using Merge
void tpool_sort(tpool_t* pool, void* arr, size_t n,size_t size,comparator compare){

    if(n <= 8192) {
        dp_quick_sort(arr,size,n,compare );
    } else {
        pool_wrapper* wrap = (pool_wrapper*) malloc(sizeof(pool_wrapper));
        job* jwrap = (job*)malloc( pool->thread_cnt * sizeof(job) );
        wrap->mutex = &pool->mutex;
        wrap->counter = pool->thread_cnt;
        pthread_cond_init(&wrap->cond,NULL);
        size_t siz = n / pool->thread_cnt;
        size_t n_manip = n;
        for(int i = 0; i < pool->thread_cnt ; i++){
            sort_wrapper* arg = malloc(sizeof(sort_wrapper));
            
            arg->arr = (arr + i * siz * size);
            arg->n = siz < n_manip ? siz : n_manip;
            arg->size = size;
            arg->compare = compare;
            jwrap[i].wrapper = wrap;

            jwrap[i].func = sort_wrapper_func;

            n_manip -= siz;
            jwrap[i].arg = arg;
            tpool_add_work(pool,tpool_wrapper,jwrap + i);

        }
        tpool_partial_wait(wrap->mutex,&wrap->cond,&wrap->counter);

        size_t init_size = n / (2 * siz);
        job* m = calloc(init_size, sizeof(job));;
        for(int o = 0; o < log(pool->thread_cnt) / log(2) ; o++){
            wrap->counter = (size_t)ceil(n / (double)(2 * siz)) ;
            for(size_t i = 0; i < n ; i += 2 * siz) {
                
                merge_wrapper arg;
                arg.arr = arr;
                arg.size = size;
                arg.compare = compare;
                arg.low = i;
                arg.mid = minsint(i + siz - 1,n - 1);
                arg.high = minsint(i + 2 * siz,n) - 1;
                m[i / (2 * siz)].arg = (void*)&arg;
                m[i / (2 * siz)].wrapper = wrap;
                m[i / (2 * siz)].func = merge_wrapper_func;
                tpool_add_work(pool,tpool_wrapper,m + (i / (2 * siz) ));
            }
            tpool_partial_wait(wrap->mutex,&wrap->cond,&wrap->counter);
            siz *= 2;
        }
        pthread_cond_destroy(&wrap->cond);
        for(int i = 0; i < pool->thread_cnt; i++){
            free(jwrap[i].arg);
        }
        free(wrap);
        free(jwrap);
    }
}