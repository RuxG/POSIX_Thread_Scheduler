
#include "so_scheduler.h"
#include "definitions.h"

static struct _thread_scheduler scheduler = { ._init = 0};

int thread_index(pthread_t id) {
    for (int i = 0; i < scheduler._num_threads; i++) {
        if (scheduler._threads[i]->_id == id) {
            return i;
        }
    }
    return -1;
}

void schedule() {

    if (scheduler._pq->_num_nodes == 0)  {
        if (scheduler._thread_on_processor != -1
            && scheduler._threads[scheduler._thread_on_processor]->_ops_on_processor == scheduler._time_quantum) {
                scheduler._threads[scheduler._thread_on_processor]->_ops_on_processor = 0;
        }
        return;   
    }

    // thread waiting in front of the queue
    struct _thread_struct *queue_head = peek(scheduler._pq);
    int head_index = thread_index(queue_head->_id);

    // schedule the head of the queue
    if (scheduler._thread_on_processor == -1) {
        pop(scheduler._pq); 

        scheduler._threads[head_index]->_signal_boolean = 1;
        scheduler._threads[head_index]->_ops_on_processor = 0;
        scheduler._threads[head_index]->_state = RUNNING;
    
        scheduler._thread_on_processor = head_index;

        pthread_cond_signal(&(scheduler._threads[head_index]->_exec_cond));
    
    // check if the head of the queue should preempt the current scheduled thread
    } else {
        if (scheduler._threads[scheduler._thread_on_processor]->_priority < queue_head->_priority ||
            (scheduler._threads[scheduler._thread_on_processor]->_priority == queue_head->_priority &&
            scheduler._threads[scheduler._thread_on_processor]->_ops_on_processor == scheduler._time_quantum)) {
            
            // preempt current thread
            scheduler._threads[scheduler._thread_on_processor]->_signal_boolean = 0;
            scheduler._threads[scheduler._thread_on_processor]->_ops_on_processor = 0;
            scheduler._threads[scheduler._thread_on_processor]->_state = READY;

            pop(scheduler._pq);
            push(scheduler._threads[scheduler._thread_on_processor], scheduler._pq); 

            // schedule head of queue
            scheduler._threads[head_index]->_signal_boolean = 1;
            scheduler._threads[head_index]->_ops_on_processor = 0;
            scheduler._threads[head_index]->_state = RUNNING;
            scheduler._thread_on_processor = head_index;
                
            pthread_cond_signal(&(scheduler._threads[head_index]->_exec_cond));
        } else {
        
            if (scheduler._threads[scheduler._thread_on_processor]->_ops_on_processor == scheduler._time_quantum) {
                scheduler._threads[scheduler._thread_on_processor]->_ops_on_processor = 0;
            }
        }
    }
}

void *start_thread(void* arg) {

    pthread_mutex_lock(&scheduler._exec_lock);

    pthread_t id = pthread_self();
    int index = thread_index(id);
    struct _thread_struct *current_thread_struct = scheduler._threads[index];

    while (!current_thread_struct->_signal_boolean)
        pthread_cond_wait(&current_thread_struct->_exec_cond, &scheduler._exec_lock);
   
    pthread_mutex_unlock(&scheduler._exec_lock);

    current_thread_struct->so_handler(current_thread_struct->_priority);

    pthread_mutex_lock(&scheduler._exec_lock);

    current_thread_struct->_state = TERMINATED;
    scheduler._thread_on_processor = -1;
    scheduler._remaining_threads--;

    if (scheduler._remaining_threads == 0) {
        push(scheduler._threads[MAIN_THREAD_INDEX], scheduler._pq);
    }

    schedule();

    pthread_mutex_unlock(&scheduler._exec_lock); 

    return (void*) NULL;
}

void init_thread_structure(pthread_t id, int index, int priority, void (*so_handler)(unsigned int)) {
    
    if (index == scheduler._max_threads) {
        scheduler._max_threads *= 2;
        scheduler._threads = realloc (scheduler._threads, scheduler._max_threads * sizeof (struct _thread_struct *));
    }

    scheduler._threads[index] = malloc (1 * sizeof (struct _thread_struct));
    scheduler._threads[index]->_id = id;
    scheduler._threads[index]->_priority = priority;
    scheduler._threads[index]->_ops_on_processor = 0;
    scheduler._threads[index]->_signal_boolean = 0;
    scheduler._threads[index]->so_handler = so_handler;
    scheduler._threads[index]->_state = READY;                         
    pthread_cond_init(&scheduler._threads[index]->_exec_cond, NULL);

    scheduler._num_threads++;
    scheduler._remaining_threads++;
}

DECL_PREFIX int so_init(unsigned int time_quantum, unsigned int io) {
    if (scheduler._init || io > SO_MAX_NUM_EVENTS || !time_quantum) return -1;

    scheduler._time_quantum = time_quantum;
    scheduler._io_no = io;

    scheduler._io_cond = malloc(io * sizeof (pthread_cond_t));
    scheduler._io_cond_booleans = calloc(io , sizeof (unsigned int));
    scheduler._threads_io_queue = malloc (io * sizeof(struct _priority_queue*));

    for (int i = 0; i < io; i++) {
        pthread_cond_init(&scheduler._io_cond[i], NULL);
        scheduler._threads_io_queue[i] = malloc (1 * sizeof(struct _priority_queue));
        scheduler._threads_io_queue[i]->_num_nodes = 0;
        scheduler._threads_io_queue[i]->_head = NULL;
    }

    pthread_mutex_init(&scheduler._exec_lock, NULL);

    scheduler._num_threads = 0;
    scheduler._remaining_threads = 0;
    scheduler._max_threads = INIT_MAX_THREADS;

    scheduler._pq = malloc (1 * sizeof(struct _priority_queue));
    scheduler._pq->_num_nodes = 0;
    scheduler._pq->_head = NULL;

    scheduler._threads = malloc( scheduler._max_threads * sizeof(struct _thread_struct *));
    scheduler._waiting_at_cond = calloc(io, sizeof(int));
    scheduler._waiting_at_cond_booleans = calloc(io, sizeof(unsigned int));
    
    pthread_t id = pthread_self();
    init_thread_structure(id, 0, 0, NULL);

    scheduler._thread_on_processor = 0;
    scheduler._threads[MAIN_THREAD_INDEX]->_signal_boolean = 1;
    scheduler._threads[MAIN_THREAD_INDEX]->_ops_on_processor++;
    scheduler._threads[MAIN_THREAD_INDEX]->_state = RUNNING;

    scheduler._init = 1;

    return 0;  
}

/*
 * creates a new so_task_t and runs it according to the scheduler
 * + handler function
 * + priority
 * returns: tid of the new task if successful or INVALID_TID
 */
DECL_PREFIX tid_t so_fork(so_handler *func, unsigned int priority) {
    if (!func || priority > SO_MAX_PRIO) return INVALID_TID;

    pthread_mutex_lock(&scheduler._exec_lock);

    // create new thread
    pthread_t new_id = -1;
    init_thread_structure(new_id, scheduler._num_threads, priority, func);

    if (pthread_create(&new_id, NULL, start_thread, NULL) < 0) {
        return INVALID_TID;
    }
    scheduler._threads[scheduler._num_threads - 1]->_id = new_id;
 
    pthread_t id = pthread_self();
    int index = thread_index(id);
    struct _thread_struct *current_thread_struct = scheduler._threads[index];
    
    current_thread_struct->_ops_on_processor++;

    // push the new thread in priority queue and schedule
    push(scheduler._threads[scheduler._num_threads - 1], scheduler._pq);
    schedule();

    // check if the thread is preempted
    while (!current_thread_struct->_signal_boolean)
         pthread_cond_wait(&current_thread_struct->_exec_cond, &scheduler._exec_lock);

    current_thread_struct->_state = RUNNING;

    pthread_mutex_unlock(&scheduler._exec_lock);
    
    return new_id; 
}

/*
 * waits for an IO device
 * + device index
 * returns: -1 if the device does not exist or 0 on success
 */
DECL_PREFIX int so_wait(unsigned int io) {
    if (io >= scheduler._io_no) return -1;

    pthread_mutex_lock(&scheduler._exec_lock);

    pthread_t id = pthread_self();
    int index = thread_index(id);
    struct _thread_struct *current_thread_struct = scheduler._threads[index];

    // increase the number of threads waiting at this event and push intro the queue
    scheduler._waiting_at_cond[io]++;
    current_thread_struct->_ops_on_processor++;
    push(current_thread_struct, scheduler._threads_io_queue[io]);

    // the thread enters a waiting state, schedule another thread
    scheduler._thread_on_processor = -1;
    current_thread_struct->_signal_boolean = 0;
    current_thread_struct->_state = WAITING;
    schedule();

    // wait for the event - the thread is in WAITING state
    while (!scheduler._io_cond_booleans[io])
        pthread_cond_wait(&scheduler._io_cond[io], &scheduler._exec_lock);

    // check if the thread is preempted
    schedule();
    while (!current_thread_struct->_signal_boolean)
        pthread_cond_wait(&current_thread_struct->_exec_cond, &scheduler._exec_lock);

    current_thread_struct->_state = RUNNING;

    pthread_mutex_unlock(&scheduler._exec_lock);
    
    return 0; 
}

/*
 * signals an IO device
 * + device index
 * return the number of tasks woke or -1 on error
 */
DECL_PREFIX int so_signal(unsigned int io) {

    if (io >= scheduler._io_no) return -1;

    pthread_mutex_lock(&scheduler._exec_lock);

    pthread_t id = pthread_self();
    int index = thread_index(id);
    struct _thread_struct *current_thread_struct = scheduler._threads[index];
     
    current_thread_struct->_ops_on_processor++;
    
    int number_of_threads_waiting = scheduler._waiting_at_cond[io];
    if (number_of_threads_waiting > 0) {

        // wake up the threads - they enter the READY state
        scheduler._io_cond_booleans[io] = 1;
        pthread_cond_broadcast(&(scheduler._io_cond[io]));
        
        // push them into priority queue to be scheduled
        while(scheduler._threads_io_queue[io]->_num_nodes > 0) {
            push(pop(scheduler._threads_io_queue[io]), scheduler._pq);
        }

        scheduler._waiting_at_cond[io] = 0;
    }

    // check if the thread is preempted
    schedule();
    while (!current_thread_struct->_signal_boolean)
        pthread_cond_wait(&current_thread_struct->_exec_cond, &scheduler._exec_lock);
    
    pthread_mutex_unlock(&scheduler._exec_lock);

    return number_of_threads_waiting; 
}

/*
 * does whatever operation
 */
DECL_PREFIX void so_exec(void) {

    pthread_mutex_lock(&scheduler._exec_lock);

    pthread_t id = pthread_self();
    unsigned int index = thread_index(id);
    struct _thread_struct *current_thread_struct = scheduler._threads[index];

    current_thread_struct->_ops_on_processor++;
    
    // check if the thread is preempted
    schedule();
    while (!current_thread_struct->_signal_boolean)
        pthread_cond_wait(&current_thread_struct->_exec_cond, &scheduler._exec_lock);

    pthread_mutex_unlock(&scheduler._exec_lock);
}

/*
 * destroys a scheduler
 */
DECL_PREFIX void so_end(void) {
    if (!scheduler._init) return;

    pthread_mutex_lock(&scheduler._exec_lock);

    pthread_t id = pthread_self();
    int index = thread_index(id);
    struct _thread_struct *current_thread_struct = scheduler._threads[index];

    scheduler._remaining_threads--;

    // schedule another thread
    if (scheduler._remaining_threads > 0) {
        scheduler._thread_on_processor = -1;
        current_thread_struct->_signal_boolean = 0;
        current_thread_struct->_state = TERMINATED;
        
        schedule();
    }

    // wait for all the other threads to be finished
    while (!current_thread_struct->_signal_boolean)
        pthread_cond_wait(&current_thread_struct->_exec_cond, &scheduler._exec_lock);
    
    pthread_mutex_unlock(&scheduler._exec_lock);

    // wait for the other threads to free their resources
    for (int i = 0; i < scheduler._num_threads; i++) {
        if (i == index) continue;
        pthread_join(scheduler._threads[i]->_id, NULL);
    }
   

    // deallocate structures
    for (int i = 0; i < scheduler._io_no; i++) {
        pthread_cond_destroy(&scheduler._io_cond[i]);
        free(scheduler._threads_io_queue[i]);
    }

    free(scheduler._io_cond);
    free(scheduler._io_cond_booleans);
    free(scheduler._threads_io_queue);
    free(scheduler._waiting_at_cond);
    free(scheduler._waiting_at_cond_booleans);
     
    while (scheduler._pq->_num_nodes > 0) {
       pop(scheduler._pq);
    }

    for (int i = 0; i < scheduler._num_threads; i++) {
        pthread_cond_destroy(&(scheduler._threads[i]->_exec_cond));
        free(scheduler._threads[i]);
        scheduler._threads[i] = NULL;
    }

    free(scheduler._threads);
    scheduler._threads = NULL;
    free(scheduler._pq);
    pthread_mutex_destroy(&scheduler._exec_lock);

    scheduler._init = 0;
   
}