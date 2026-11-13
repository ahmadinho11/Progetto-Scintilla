#ifndef COMMON_H
#define COMMON_H

#define _POSIX_C_SOURCE 200809L
#define _XOPEN_SOURCE 700

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <errno.h>
#include <ctype.h>
#include <time.h>

#define MAX_JOBS 128
#define MAX_NAME_LEN 32
#define MAX_GANTT_SEGMENTS 2048

#define SHM_NAME "/mini_cpu_sched_shm"
#define SEM_NAME "/mini_cpu_sched_sem"
#define LOG_FILE "schedule.log"

#define DEFAULT_TIME_UNIT_MS 50

typedef enum {
    ALG_FCFS,
    ALG_SJF,
    ALG_SRTF,
    ALG_RR,
    ALG_PRIORITY,
    ALG_ALL
} AlgorithmType;

/* Structure representing a Job during live execution */
typedef struct {
    char id[MAX_NAME_LEN];
    int arrival_time;
    int burst_time;
    int remaining_time;
    int first_start_time;   /* -1 if not started yet */
    int completion_time;
    int turnaround_time;
    int waiting_time;
    int response_time;
    int priority;           /* Optional bonus */
    int has_started;
    int is_completed;
    int terminate;          /* Thread exit signal */
    sem_t run_sem;          /* Dispatcher -> Job handoff semaphore */
    pthread_t thread;
} Job;

/* Structure representing a Gantt chart continuous segment */
typedef struct {
    char job_id[MAX_NAME_LEN];
    int start_time;
    int end_time;
} GanttSegment;

/* Job record stored in shared memory for report process */
typedef struct {
    char id[MAX_NAME_LEN];
    int arrival_time;
    int burst_time;
    int completion_time;
    int turnaround_time;
    int waiting_time;
    int response_time;
    int priority;
} JobRecord;

/* Shared memory payload handed to report process */
typedef struct {
    char algorithm[32];
    int quantum;
    int total_jobs;
    JobRecord jobs[MAX_JOBS];
    int total_time;
    int context_switches;
    double avg_turnaround;
    double avg_waiting;
    double avg_response;
    int gantt_count;
    GanttSegment gantt[MAX_GANTT_SEGMENTS];
} ReportData;

/* Comparison record for ALL mode */
typedef struct {
    char algorithm_name[32];
    double avg_waiting;
    double avg_turnaround;
    double avg_response;
    int context_switches;
} ComparisonRecord;

#endif /* COMMON_H */
