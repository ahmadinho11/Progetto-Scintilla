#include "common.h"

/* Global / Shared State */
static Job job_table[MAX_JOBS];
static int total_jobs = 0;
static int time_unit_us = DEFAULT_TIME_UNIT_MS * 1000;
static int global_time = 0;

/* Pipe to logger process */
static int logger_pipe_fd = -1;
static pid_t logger_pid = -1;

/* Synchronization tools */
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t done_sem, arrival_trigger_sem, arrival_done_sem;
static int simulation_running = 1;

/* Ready Queue */
typedef struct {
    Job *items[MAX_JOBS];
    int count;
} ReadyQueue;

static ReadyQueue ready_queue;

/* Gantt recording */
static GanttSegment gantt_log[MAX_GANTT_SEGMENTS];
static int gantt_count = 0;

/* --- Logging Helper --- */
static void log_event(const char *format, ...) {
    pthread_mutex_lock(&log_mutex);
    if (logger_pipe_fd >= 0) {
        char msg[512];
        va_list args;
        va_start(args, format);
        vsnprintf(msg, sizeof(msg), format, args);
        va_end(args);
        if (write(logger_pipe_fd, msg, strlen(msg)) < 0) {}
    }
    pthread_mutex_unlock(&log_mutex);
}

/* --- Gantt Helper --- */
static void record_gantt_unit(const char *job_id, int t) {
    if (gantt_count > 0 && strcmp(gantt_log[gantt_count - 1].job_id, job_id) == 0 &&
        gantt_log[gantt_count - 1].end_time == t) {
        gantt_log[gantt_count - 1].end_time = t + 1;
    } else if (gantt_count < MAX_GANTT_SEGMENTS) {
        strncpy(gantt_log[gantt_count].job_id, job_id, MAX_NAME_LEN - 1);
        gantt_log[gantt_count].job_id[MAX_NAME_LEN - 1] = '\0';
        gantt_log[gantt_count].start_time = t;
        gantt_log[gantt_count].end_time = t + 1;
        gantt_count++;
    }
}

/* --- Ready Queue Operations --- */
static void queue_enqueue(ReadyQueue *q, Job *job) {
    if (q->count < MAX_JOBS) q->items[q->count++] = job;
}

static Job *queue_remove_at(ReadyQueue *q, int index) {
    if (index < 0 || index >= q->count) return NULL;
    Job *job = q->items[index];
    for (int i = index; i < q->count - 1; i++) q->items[i] = q->items[i + 1];
    q->count--;
    return job;
}

/* Unified Job Comparator (tie-breaking: earlier arrival, then smaller ID) */
static int job_cmp(const Job *a, const Job *b, AlgorithmType alg) {
    int diff = (alg == ALG_SJF) ? (a->burst_time - b->burst_time) :
               (alg == ALG_SRTF) ? (a->remaining_time - b->remaining_time) :
               (alg == ALG_PRIORITY) ? (a->priority - b->priority) : 0;
    if (diff != 0) return diff;
    if (a->arrival_time != b->arrival_time) return a->arrival_time - b->arrival_time;
    return strcmp(a->id, b->id);
}

static int queue_find_best(ReadyQueue *q, AlgorithmType alg) {
    if (q->count == 0) return -1;
    int best = 0;
    for (int i = 1; i < q->count; i++) {
        if (job_cmp(q->items[i], q->items[best], alg) < 0) best = i;
    }
    return best;
}

/* --- Job Thread Function (Part 3) --- */
static void *job_thread_func(void *arg) {
    Job *job = (Job *)arg;
    while (1) {
        sem_wait(&job->run_sem);
        if (job->terminate) break;

        printf("[t= %2d] %s running (remaining %d)\n", global_time, job->id, job->remaining_time);
        fflush(stdout);

        if (time_unit_us > 0) usleep(time_unit_us);
        sem_post(&done_sem);
    }
    return NULL;
}

/* --- Arrival Thread Function (Part 3) --- */
static void *arrival_thread_func(void *arg) {
    (void)arg;
    int arrived_mask[MAX_JOBS] = {0};

    while (simulation_running) {
        sem_wait(&arrival_trigger_sem);
        if (!simulation_running) break;

        pthread_mutex_lock(&queue_mutex);
        for (int i = 0; i < total_jobs; i++) {
            if (!arrived_mask[i] && job_table[i].arrival_time <= global_time) {
                arrived_mask[i] = 1;
                queue_enqueue(&ready_queue, &job_table[i]);
                log_event("[t= %2d] Event: Job %s arrived (burst=%d, prio=%d)\n",
                          global_time, job_table[i].id, job_table[i].burst_time, job_table[i].priority);
            }
        }
        pthread_mutex_unlock(&queue_mutex);
        sem_post(&arrival_done_sem);
    }
    return NULL;
}

static int job_init_cmp(const void *a, const void *b) {
    const Job *j1 = (const Job *)a, *j2 = (const Job *)b;
    if (j1->arrival_time != j2->arrival_time) return j1->arrival_time - j2->arrival_time;
    return strcmp(j1->id, j2->id);
}

/* --- Input File Parsing & Validation (Part 1) --- */
static int parse_input_file(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "Error: Cannot open input file '%s': %s\n", filename, strerror(errno));
        return -1;
    }

    char line[256];
    int line_num = 0;
    total_jobs = 0;

    while (fgets(line, sizeof(line), fp)) {
        line_num++;
        char *ptr = line;
        while (isspace((unsigned char)*ptr)) ptr++;
        if (*ptr == '\0' || *ptr == '#') continue;

        char id[MAX_NAME_LEN];
        int arrival = 0, burst = 0, priority = 0;
        int parsed = sscanf(ptr, "%31s %d %d %d", id, &arrival, &burst, &priority);
        if (parsed < 3 || arrival < 0 || burst <= 0) {
            fprintf(stderr, "Error at %s:%d: Invalid line data (expected: id arrival burst [priority])\n",
                    filename, line_num);
            fclose(fp);
            return -1;
        }

        for (int i = 0; i < total_jobs; i++) {
            if (strcmp(job_table[i].id, id) == 0) {
                fprintf(stderr, "Error at %s:%d: Duplicate job ID detected ('%s')\n", filename, line_num, id);
                fclose(fp);
                return -1;
            }
        }

        if (total_jobs >= MAX_JOBS) {
            fprintf(stderr, "Error: Exceeded maximum supported jobs (%d)\n", MAX_JOBS);
            fclose(fp);
            return -1;
        }

        Job *j = &job_table[total_jobs++];
        memset(j, 0, sizeof(Job));
        strncpy(j->id, id, MAX_NAME_LEN - 1);
        j->arrival_time = arrival;
        j->burst_time = burst;
        j->remaining_time = burst;
        j->first_start_time = -1;
        j->priority = (parsed >= 4) ? priority : 0;
    }

    fclose(fp);
    if (total_jobs == 0) {
        fprintf(stderr, "Error: Input file '%s' contains no valid jobs.\n", filename);
        return -1;
    }

    /* Ensure jobs are sorted by arrival time, and by ID for ties */
    qsort(job_table, total_jobs, sizeof(Job), job_init_cmp);
    return 0;
}

/* --- Reset Job State for Reruns (e.g. in ALL mode) --- */
static void reset_job_state(void) {
    for (int i = 0; i < total_jobs; i++) {
        job_table[i].remaining_time = job_table[i].burst_time;
        job_table[i].first_start_time = -1;
        job_table[i].completion_time = job_table[i].turnaround_time = 0;
        job_table[i].waiting_time = job_table[i].response_time = 0;
        job_table[i].has_started = job_table[i].terminate = 0;
    }
    gantt_count = 0;
    global_time = 0;
    ready_queue.count = 0;
}

/* --- Logger Process Setup (Part 5: fork + dup2) --- */
static int start_logger_process(void) {
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("Failed to create pipe for logger");
        return -1;
    }

    logger_pid = fork();
    if (logger_pid < 0) {
        perror("Failed to fork logger process");
        close(pipefd[0]);
        close(pipefd[1]);
        return -1;
    }

    if (logger_pid == 0) {
        close(pipefd[1]);
        if (dup2(pipefd[0], STDIN_FILENO) < 0) {
            perror("dup2 failed in logger process");
            close(pipefd[0]);
            exit(1);
        }
        close(pipefd[0]);
        execl("./logger", "logger", NULL);
        execl("./bin/logger", "logger", NULL);
        exit(1);
    }

    close(pipefd[0]);
    logger_pipe_fd = pipefd[1];
    return 0;
}

static void stop_logger_process(void) {
    if (logger_pipe_fd >= 0) {
        close(logger_pipe_fd);
        logger_pipe_fd = -1;
    }
    if (logger_pid > 0) {
        waitpid(logger_pid, NULL, 0);
        logger_pid = -1;
    }
}

/* --- Shared Memory & Report Process (Part 6) --- */
static int trigger_report_process(const char *alg_name, int quantum, int context_switches, ComparisonRecord *comp_out) {
    sem_unlink(SEM_NAME);
    shm_unlink(SHM_NAME);

    sem_t *report_sem = sem_open(SEM_NAME, O_CREAT | O_EXCL, 0666, 0);
    if (report_sem == SEM_FAILED) {
        perror("sem_open failed");
        return -1;
    }

    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR | O_TRUNC, 0666);
    if (shm_fd < 0 || ftruncate(shm_fd, sizeof(ReportData)) < 0) {
        perror("shm setup failed");
        if (shm_fd >= 0) close(shm_fd);
        sem_close(report_sem);
        sem_unlink(SEM_NAME);
        shm_unlink(SHM_NAME);
        return -1;
    }

    ReportData *data = (ReportData *)mmap(NULL, sizeof(ReportData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    if (data == MAP_FAILED) {
        perror("mmap failed");
        close(shm_fd);
        sem_close(report_sem);
        sem_unlink(SEM_NAME);
        shm_unlink(SHM_NAME);
        return -1;
    }

    memset(data, 0, sizeof(ReportData));
    strncpy(data->algorithm, alg_name, sizeof(data->algorithm) - 1);
    data->quantum = quantum;
    data->total_jobs = total_jobs;
    data->total_time = global_time;
    data->context_switches = context_switches;

    double sum_turnaround = 0.0, sum_waiting = 0.0, sum_response = 0.0;
    for (int i = 0; i < total_jobs; i++) {
        JobRecord *rec = &data->jobs[i];
        strncpy(rec->id, job_table[i].id, MAX_NAME_LEN - 1);
        rec->arrival_time = job_table[i].arrival_time;
        rec->burst_time = job_table[i].burst_time;
        rec->completion_time = job_table[i].completion_time;
        rec->turnaround_time = job_table[i].turnaround_time;
        rec->waiting_time = job_table[i].waiting_time;
        rec->response_time = job_table[i].response_time;
        rec->priority = job_table[i].priority;

        sum_turnaround += rec->turnaround_time;
        sum_waiting += rec->waiting_time;
        sum_response += rec->response_time;
    }

    data->avg_turnaround = total_jobs > 0 ? (sum_turnaround / total_jobs) : 0.0;
    data->avg_waiting = total_jobs > 0 ? (sum_waiting / total_jobs) : 0.0;
    data->avg_response = total_jobs > 0 ? (sum_response / total_jobs) : 0.0;

    data->gantt_count = gantt_count;
    memcpy(data->gantt, gantt_log, gantt_count * sizeof(GanttSegment));

    if (comp_out) {
        strncpy(comp_out->algorithm_name, alg_name, sizeof(comp_out->algorithm_name) - 1);
        comp_out->avg_waiting = data->avg_waiting;
        comp_out->avg_turnaround = data->avg_turnaround;
        comp_out->avg_response = data->avg_response;
        comp_out->context_switches = context_switches;
    }

    /* Signal report process to read data */
    sem_post(report_sem);

    pid_t report_pid = fork();
    if (report_pid < 0) {
        perror("fork report process failed");
    } else if (report_pid == 0) {
        execl("./report", "report", NULL);
        execl("./bin/report", "report", NULL);
        perror("execl ./report failed");
        exit(1);
    } else {
        waitpid(report_pid, NULL, 0);
    }

    munmap(data, sizeof(ReportData));
    close(shm_fd);
    shm_unlink(SHM_NAME);
    sem_close(report_sem);
    sem_unlink(SEM_NAME);
    return 0;
}

/* --- Core Live Dispatcher Engine --- */
static int run_scheduler(AlgorithmType alg, int quantum, ComparisonRecord *comp_out) {
    reset_job_state();

    const char *alg_name = (alg == ALG_SJF) ? "SJF" :
                           (alg == ALG_SRTF) ? "SRTF" :
                           (alg == ALG_RR) ? "Round Robin" :
                           (alg == ALG_PRIORITY) ? "Priority" : "FCFS";

    printf("\n================================================================================\n");
    printf("   STARTING LIVE SCHEDULER: %s", alg_name);
    if (alg == ALG_RR) printf(" (Quantum = %d)", quantum);
    printf("\n================================================================================\n");

    start_logger_process();
    log_event("========== Starting Simulation: %s (jobs=%d) ==========\n", alg_name, total_jobs);

    sem_init(&done_sem, 0, 0);
    sem_init(&arrival_trigger_sem, 0, 0);
    sem_init(&arrival_done_sem, 0, 0);
    simulation_running = 1;

    /* Create job threads */
    for (int i = 0; i < total_jobs; i++) {
        sem_init(&job_table[i].run_sem, 0, 0);
        if (pthread_create(&job_table[i].thread, NULL, job_thread_func, &job_table[i]) != 0) {
            perror("Failed to create job thread");
            return -1;
        }
    }

    /* Create arrival thread */
    pthread_t arrival_thread;
    if (pthread_create(&arrival_thread, NULL, arrival_thread_func, NULL) != 0) {
        perror("Failed to create arrival thread");
        return -1;
    }

    int completed_jobs = 0, context_switches = 0, current_quantum_used = 0;
    Job *last_running_job = NULL, *current_running_job = NULL, *rr_preempted_job = NULL;

    /* Dispatcher main loop */
    while (completed_jobs < total_jobs) {
        /* Step 1: Check arriving jobs at global_time */
        sem_post(&arrival_trigger_sem);
        sem_wait(&arrival_done_sem);

        /* Step 2: Newly arrived jobs enter queue before preempted job */
        if (rr_preempted_job != NULL) {
            pthread_mutex_lock(&queue_mutex);
            queue_enqueue(&ready_queue, rr_preempted_job);
            pthread_mutex_unlock(&queue_mutex);
            rr_preempted_job = NULL;
        }

        /* Step 3: Algorithm selection */
        pthread_mutex_lock(&queue_mutex);
        if (alg == ALG_FCFS || alg == ALG_RR) {
            if (current_running_job == NULL) {
                current_running_job = queue_remove_at(&ready_queue, 0);
                current_quantum_used = 0;
            }
        } else if (alg == ALG_SJF || alg == ALG_PRIORITY) {
            if (current_running_job == NULL) {
                current_running_job = queue_remove_at(&ready_queue, queue_find_best(&ready_queue, alg));
            }
        } else if (alg == ALG_SRTF) {
            int best_idx = queue_find_best(&ready_queue, alg);
            if (best_idx >= 0) {
                Job *candidate = ready_queue.items[best_idx];
                if (current_running_job == NULL) {
                    current_running_job = queue_remove_at(&ready_queue, best_idx);
                } else if (candidate->remaining_time < current_running_job->remaining_time) {
                    log_event("[t= %2d] Event: %s preempted by shorter job %s (rem %d < %d)\n",
                              global_time, current_running_job->id, candidate->id,
                              candidate->remaining_time, current_running_job->remaining_time);
                    queue_enqueue(&ready_queue, current_running_job);
                    current_running_job = queue_remove_at(&ready_queue, best_idx);
                }
            }
        }
        pthread_mutex_unlock(&queue_mutex);

        /* Step 4: Dispatch job or Handle Idle CPU */
        if (current_running_job == NULL) {
            printf("[t= %2d] CPU IDLE\n", global_time);
            fflush(stdout);
            log_event("[t= %2d] Event: CPU is IDLE\n", global_time);
            record_gantt_unit("IDLE", global_time);

            if (time_unit_us > 0) usleep(time_unit_us);
            last_running_job = NULL;
            global_time++;
            continue;
        }

        /* Context switch count (first dispatch and switches to/from IDLE excluded) */
        if (last_running_job != NULL && last_running_job != current_running_job) {
            context_switches++;
            printf("[t= %2d] ---- context switch: %s -> %s ----\n",
                   global_time, last_running_job->id, current_running_job->id);
            fflush(stdout);
            log_event("[t= %2d] Event: Context switch %s -> %s\n",
                      global_time, last_running_job->id, current_running_job->id);
        }

        /* Track response time */
        if (!current_running_job->has_started) {
            current_running_job->has_started = 1;
            current_running_job->first_start_time = global_time;
            current_running_job->response_time = global_time - current_running_job->arrival_time;
            log_event("[t= %2d] Event: %s started\n", global_time, current_running_job->id);
        } else if (last_running_job != current_running_job) {
            log_event("[t= %2d] Event: %s resumed\n", global_time, current_running_job->id);
        }

        /* Step 5: Dispatcher-to-Job handoff via semaphores */
        sem_post(&current_running_job->run_sem);
        sem_wait(&done_sem);

        /* Step 6: Post-execution update */
        current_running_job->remaining_time--;
        record_gantt_unit(current_running_job->id, global_time);
        last_running_job = current_running_job;
        current_quantum_used++;

        if (current_running_job->remaining_time == 0) {
            current_running_job->completion_time = global_time + 1;
            current_running_job->turnaround_time = current_running_job->completion_time - current_running_job->arrival_time;
            current_running_job->waiting_time = current_running_job->turnaround_time - current_running_job->burst_time;

            log_event("[t= %2d] Event: %s finished (turnaround=%d, wait=%d)\n",
                      global_time + 1, current_running_job->id,
                      current_running_job->turnaround_time, current_running_job->waiting_time);

            completed_jobs++;
            current_running_job = NULL;
            current_quantum_used = 0;
        } else if (alg == ALG_RR && current_quantum_used >= quantum) {
            log_event("[t= %2d] Event: %s preempted (quantum expired, remaining=%d)\n",
                      global_time + 1, current_running_job->id, current_running_job->remaining_time);
            rr_preempted_job = current_running_job;
            current_running_job = NULL;
            current_quantum_used = 0;
        }

        global_time++;
    }

    /* Terminate job threads */
    for (int i = 0; i < total_jobs; i++) {
        job_table[i].terminate = 1;
        sem_post(&job_table[i].run_sem);
        pthread_join(job_table[i].thread, NULL);
        sem_destroy(&job_table[i].run_sem);
    }

    /* Terminate arrival thread */
    simulation_running = 0;
    sem_post(&arrival_trigger_sem);
    pthread_join(arrival_thread, NULL);

    sem_destroy(&done_sem);
    sem_destroy(&arrival_trigger_sem);
    sem_destroy(&arrival_done_sem);

    log_event("========== Simulation Completed: %s ==========\n\n", alg_name);
    stop_logger_process();

    /* Trigger report process through shared memory and exec */
    trigger_report_process(alg_name, (alg == ALG_RR) ? quantum : 0, context_switches, comp_out);
    return 0;
}

/* --- Comparison Table Display for ALL mode --- */
static void print_comparison_table(ComparisonRecord records[], int count) {
    printf("\n================================================================================\n");
    printf("                           ALGORITHM COMPARISON TABLE\n");
    printf("================================================================================\n");
    printf("%-22s %-14s %-16s %-14s %-16s\n",
           "Algorithm", "Avg waiting", "Avg turnaround", "Avg response", "Context switches");
    printf("--------------------------------------------------------------------------------\n");

    for (int i = 0; i < count; i++) {
        printf("%-22s %-14.2f %-16.2f %-14.2f %-16d\n",
               records[i].algorithm_name,
               records[i].avg_waiting,
               records[i].avg_turnaround,
               records[i].avg_response,
               records[i].context_switches);
    }
    printf("================================================================================\n\n");
}

/* --- Usage instructions --- */
static void print_usage(const char *prog) {
    printf("Usage: %s <input_file> <algorithm> [quantum]\n", prog);
    printf("  algorithm : fcfs | sjf | srtf | rr | priority | all\n");
    printf("  quantum   : required for 'rr', optional for 'all' (default: 2)\n");
    printf("Examples:\n");
    printf("  %s jobs1.txt fcfs\n", prog);
    printf("  %s jobs1.txt rr 2\n", prog);
    printf("  %s jobs1.txt all 2\n", prog);
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    const char *input_file = argv[1];
    const char *alg_str = argv[2];
    int quantum = 2;

    if (argc >= 4) {
        quantum = atoi(argv[3]);
        if (quantum <= 0) {
            fprintf(stderr, "Error: Quantum must be a positive integer (> 0)\n");
            return 1;
        }
    }

    char *env_speed = getenv("TIME_UNIT_MS");
    if (env_speed) {
        int ms = atoi(env_speed);
        if (ms >= 0) time_unit_us = ms * 1000;
    }

    if (parse_input_file(input_file) != 0) return 1;

    AlgorithmType alg = ALG_ALL;
    if (strcasecmp(alg_str, "fcfs") == 0) alg = ALG_FCFS;
    else if (strcasecmp(alg_str, "sjf") == 0) alg = ALG_SJF;
    else if (strcasecmp(alg_str, "srtf") == 0) alg = ALG_SRTF;
    else if (strcasecmp(alg_str, "rr") == 0) {
        alg = ALG_RR;
        if (argc < 4) printf("[Note] No quantum specified for Round Robin; using default quantum = 2\n");
    } else if (strcasecmp(alg_str, "priority") == 0) alg = ALG_PRIORITY;
    else if (strcasecmp(alg_str, "all") != 0) {
        fprintf(stderr, "Error: Unknown algorithm '%s'\n", alg_str);
        print_usage(argv[0]);
        return 1;
    }

    if (alg == ALG_ALL) {
        AlgorithmType all_algs[] = {ALG_FCFS, ALG_SJF, ALG_SRTF, ALG_RR};
        ComparisonRecord comp_records[4];
        for (int i = 0; i < 4; i++) {
            int q = (all_algs[i] == ALG_RR) ? quantum : 0;
            run_scheduler(all_algs[i], q, &comp_records[i]);
            if (all_algs[i] == ALG_RR) {
                snprintf(comp_records[i].algorithm_name, sizeof(comp_records[i].algorithm_name), "Round Robin (q = %d)", quantum);
            }
        }
        print_comparison_table(comp_records, 4);
    } else {
        run_scheduler(alg, quantum, NULL);
    }

    return 0;
}
