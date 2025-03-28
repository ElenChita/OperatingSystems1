#ifndef KIWI_H
#define KIWI_H

#include <pthread.h>
#include "../engine/db.h"

// Create struct for the args
typedef struct {
    int count;                    // num of data
    int r;                      // some arg r
    pthread_mutex_t* read_lock;   // mutex for sychronism
    DB* db;
    int thread_num;
} ThreadArgs; 

/*typedef struct {
    long int count;
    int r;
    pthread_mutex_t *engine_lock;
    DB* db;
} ThreadArgs; */

/*typedef struct  //the args given for the pthread_create
{
    long int count;
    int r;
    int thread_num;
} ThreadArgs; */

// Declare functions
void _write_test(void* args);
void _read_test(void* args);

#endif // KIWI_H
