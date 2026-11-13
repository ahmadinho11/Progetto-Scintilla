#include "common.h"

int main(int argc, char *argv[]) {
    (void)argc;
    (void)argv;

    FILE *log_file = fopen(LOG_FILE, "a");
    if (!log_file) {
        perror("[Logger] Failed to open schedule.log");
        return 1;
    }

    char buffer[512];
    /* Logger reads events from standard input (redirected from scheduler pipe via dup2) */
    while (fgets(buffer, sizeof(buffer), stdin) != NULL) {
        fputs(buffer, log_file);
        fflush(log_file);
    }

    fclose(log_file);
    return 0;
}
