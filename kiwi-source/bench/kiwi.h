#ifndef KIWI_H
#define KIWI_H

#include <pthread.h>

// Create struct for the args
typedef struct {
    int count;                    // num of data
    void* r;                      // some arg r
    pthread_mutex_t* read_lock;   // mutex for sychronism
} ThreadArgs;

// Declare functions
void _write_test(void* args);
void _read_test(void* args);

#endif // KIWI_H
