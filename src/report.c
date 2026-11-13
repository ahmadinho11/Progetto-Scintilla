#include "common.h"

static void print_gantt_chart(const ReportData *data) {
    if (data->gantt_count == 0) {
        printf("No execution recorded in Gantt chart.\n");
        return;
    }

    printf("\n================================================================================\n");
    printf("                               GANTT CHART\n");
    printf("================================================================================\n");

    /* Concise list representation */
    printf("Timeline Sequence:\n");
    for (int i = 0; i < data->gantt_count; i++) {
        printf(" [%d - %d: %s]", data->gantt[i].start_time, data->gantt[i].end_time, data->gantt[i].job_id);
        if ((i + 1) % 6 == 0 && (i + 1) < data->gantt_count) {
            printf("\n");
        }
    }
    printf("\n\n");

    /* Formatted ASCII Gantt blocks (wrapped in chunks if needed) */
    const int CHUNK_SIZE = 10;
    for (int start_idx = 0; start_idx < data->gantt_count; start_idx += CHUNK_SIZE) {
        int end_idx = start_idx + CHUNK_SIZE;
        if (end_idx > data->gantt_count) end_idx = data->gantt_count;

        /* Top border */
        for (int i = start_idx; i < end_idx; i++) {
            printf("+---------");
        }
        printf("+\n");

        /* Job labels inside boxes */
        for (int i = start_idx; i < end_idx; i++) {
            printf("| %-7s ", data->gantt[i].job_id);
        }
        printf("|\n");

        /* Bottom border */
        for (int i = start_idx; i < end_idx; i++) {
            printf("+---------");
        }
        printf("+\n");

        /* Timeline marks below boxes */
        for (int i = start_idx; i < end_idx; i++) {
            printf("%-10d", data->gantt[i].start_time);
        }
        printf("%d\n\n", data->gantt[end_idx - 1].end_time);
    }
}

static void print_metrics_table(const ReportData *data) {
    printf("================================================================================\n");
    printf("                           PER-JOB METRICS REPORT\n");
    printf("================================================================================\n");
    printf("%-8s %-10s %-8s %-12s %-12s %-10s %-10s\n",
           "Job", "Arrival", "Burst", "Completion", "Turnaround", "Waiting", "Response");
    printf("--------------------------------------------------------------------------------\n");

    for (int i = 0; i < data->total_jobs; i++) {
        const JobRecord *j = &data->jobs[i];
        printf("%-8s %-10d %-8d %-12d %-12d %-10d %-10d\n",
               j->id, j->arrival_time, j->burst_time,
               j->completion_time, j->turnaround_time, j->waiting_time, j->response_time);
    }

    printf("--------------------------------------------------------------------------------\n");
    printf("%-28s %-12.2f %-10.2f %-10.2f\n",
           "Average", data->avg_turnaround, data->avg_waiting, data->avg_response);
    printf("================================================================================\n");
    printf("Total Execution Time : %d time units\n", data->total_time);
    printf("Context Switches     : %d\n", data->context_switches);
    printf("================================================================================\n\n");
}

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    /* Part 6: Wait on named semaphore posted by scheduler after writing data */
    sem_t *report_sem = sem_open(SEM_NAME, 0);
    if (report_sem == SEM_FAILED) {
        perror("[Report Process] sem_open failed");
        return 1;
    }

    if (sem_wait(report_sem) != 0) {
        perror("[Report Process] sem_wait failed");
        sem_close(report_sem);
        return 1;
    }

    /* Attach to POSIX shared memory segment */
    int shm_fd = shm_open(SHM_NAME, O_RDONLY, 0666);
    if (shm_fd < 0) {
        perror("[Report Process] shm_open failed");
        sem_close(report_sem);
        return 1;
    }

    ReportData *data = (ReportData *)mmap(NULL, sizeof(ReportData), PROT_READ, MAP_SHARED, shm_fd, 0);
    if (data == MAP_FAILED) {
        perror("[Report Process] mmap failed");
        close(shm_fd);
        sem_close(report_sem);
        return 1;
    }

    /* Print Header */
    printf("\n");
    printf("################################################################################\n");
    printf("             REPORT PROCESS (PID: %d) EXEC'D SUCCESSFULLY\n", getpid());
    printf("             ALGORITHM: %s", data->algorithm);
    if (data->quantum > 0) {
        printf(" (quantum = %d)", data->quantum);
    }
    printf("\n");
    printf("################################################################################\n");

    print_gantt_chart(data);
    print_metrics_table(data);

    /* Clean up local mappings */
    munmap(data, sizeof(ReportData));
    close(shm_fd);
    sem_close(report_sem);

    return 0;
}
