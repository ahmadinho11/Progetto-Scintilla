================================================================================
OPERATING SYSTEMS LAB | MINI CPU SCHEDULER
Project: Progetto Scintilla (Mini CPU Scheduler)
================================================================================

1. GROUP MEMBERS & CONTRIBUTION

Group Members:
- Ahmad Raza (Roll No: 24i-2574) - Scheduler Dispatcher, Thread Management, Semaphores, Logger Process (Parts 2,3 & 3)
- Ali Farooq (Roll No: 24i-2576) - IPC Pipes, Input Parsing, Job Table & Shared Memory Report, Metrics & Gantt Chart

(Note: Every group member contributed to the architectural design, synchronization
protocol, testing, and debugging.)


2. HOW TO COMPILE AND RUN ON UBUNTU / LINUX

Prerequisites on Ubuntu:
  sudo apt-get update
  sudo apt-get install build-essential

Compilation:
  make clean
  make all

This will compile three binaries with -Wall -Wextra -pthread -std=c11:
  - scheduler : The main dispatcher and multi-threaded simulation engine
  - report    : The standalone reporter invoked via fork() + exec()
  - logger    : The background logging process reading events from pipe/stdin

Command Line Syntax:
  ./scheduler <input_file> <algorithm> [quantum]

Supported Algorithms:
  fcfs     : First-Come, First-Served (non-preemptive)
  sjf      : Shortest Job First (non-preemptive)
  srtf     : Shortest Remaining Time First (preemptive at time boundaries)
  rr       : Round Robin (preemptive with time quantum, default q = 2)
  priority : Priority scheduling (lower number = higher priority)
  all      : Runs all algorithms on the same dataset and prints comparison table

Example Commands:
  ./scheduler inputs/jobs1.txt fcfs
  ./scheduler inputs/jobs1.txt sjf
  ./scheduler inputs/jobs1.txt srtf
  ./scheduler inputs/jobs1.txt rr 2
  ./scheduler inputs/jobs1.txt all 2

Running with Custom Speed:
  By default, 1 time unit = 50 ms. You can control simulation speed using the
  TIME_UNIT_MS environment variable:
  TIME_UNIT_MS=10 ./scheduler inputs/jobs1.txt all 2   # Fast 10ms per unit
  TIME_UNIT_MS=200 ./scheduler inputs/jobs1.txt rr 2   # Lab default 200ms


3. SYNCHRONISATION TOOLS & ARCHITECTURE
A. Mutexes:
   - queue_mutex (pthread_mutex_t):
     Protects access to the shared Ready Queue (enqueue, dequeue, search, and
     job extraction). It coordinates between the Arrival Thread (which enqueues
     jobs as their arrival time is reached) and the Dispatcher Thread (which
     selects and dequeues jobs).
   - log_mutex (pthread_mutex_t):
     Protects concurrent writes to the logger IPC pipe.

B. Semaphores:
   - Job Run Semaphores (sem_t job->run_sem, initial value = 0):
     Each job has its own dedicated POSIX semaphore. The job thread blocks on
     sem_wait(&job->run_sem). The dispatcher posts sem_post(&selected_job->run_sem)
     to allocate 1 time unit of CPU execution to that thread.
   - Done Semaphore (sem_t done_sem, initial value = 0):
     After executing 1 time unit and printing its status line, the active job
     thread posts sem_post(&done_sem). The dispatcher blocks on sem_wait(&done_sem)
     until the job finishes its time slice, ensuring strict lock-step execution.
   - Arrival Handshake Semaphores (arrival_trigger_sem & arrival_done_sem, initial = 0):
     Used to synchronize the arrival thread with the simulated time clock.
     At each clock tick t, the dispatcher signals arrival_trigger_sem. The arrival
     thread enqueues any jobs arriving at t and signals arrival_done_sem before
     the dispatcher makes scheduling choices.
   - Report Named Semaphore (sem_t report_sem at "/mini_cpu_sched_sem", initial = 0):
     Used between the main scheduler process and the separate exec'd report process.
     The report process calls sem_wait(report_sem) so it reads the final metrics
     only AFTER the scheduler has completely written them into shared memory.

C. Shared Memory Choice:
   - We chose POSIX Shared Memory (shm_open, ftruncate, mmap, munmap, shm_unlink)
     located at "/mini_cpu_sched_shm".
   - The ReportData structure contains all completed job metrics, averages,
     context switch counts, and the Gantt chart segments.

D. IPC Pipe & Logger:
   - The scheduler creates a unidirectional pipe with pipe().
   - fork() spawns the logger process.
   - dup2(pipefd[0], STDIN_FILENO) redirects the read end of the pipe to stdin.
   - The logger process appends events into schedule.log in real time.
   - Unused pipe ends are closed. When scheduler closes write end, logger detects
     EOF, flushes, and cleanly terminates. waitpid() reaps child (no zombies).


4. ASSUMPTIONS, CONVENTIONS & KNOWN LIMITATIONS
--------------------------------------------------------------------------------
- Tie-breaking:
  If two jobs are equal under an algorithm, the job that arrived earlier is picked.
  If arrival times are also identical, the job with lexicographically smaller ID
  is picked (e.g., P1 before P2).
- Round Robin preemption:
  If a new job arrives at the exact same moment that a running job's quantum expires,
  the newly arrived job is enqueued BEFORE the preempted job.
- SRTF preemption:
  Remaining times are checked at every time-unit boundary. If a ready job has strictly
  smaller remaining time, preemption occurs. If remaining times are equal, the
  currently running job continues without preemption.
- Context switches:
  Counted each time the CPU switches from one job to a different job.
  The initial dispatch from empty state and transitions to/from IDLE are NOT counted.
- Idle CPU:
  When the ready queue is empty while jobs remain uncompleted, the CPU state is IDLE.
  IDLE slices are displayed on console and recorded in the Gantt chart.
- Known Limitations:
  * Maximum supported concurrent jobs is 128 (configured via MAX_JOBS in common.h).
  * Arrival and burst durations are whole non-negative integers.
  * Designed for POSIX/Linux environments (requires pthreads, POSIX semaphores, and shm_open).


5. TEST INPUT FILES
- inputs/jobs1.txt: Project brief example (P1 0 5, P2 1 3, P3 2 8).
- inputs/jobs2.txt: All 5 jobs arrive at time 0 (Convoy effect demonstration).
- inputs/jobs3.txt: Contains an idle gap (queue becomes empty between t=5 and t=10).
- inputs/jobs4.txt: Starvation test case showing SRTF vs FCFS differences.
