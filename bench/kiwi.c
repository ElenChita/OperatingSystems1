#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include "kiwi.h"
#include <pthread.h>
#include "../engine/sst.h"

#define DATAS ("testdb")


void _write_test(void* args) {
    ThreadArgs* t_args = (ThreadArgs*)args; // Μετατροπή των παραμέτρων
    long int count = t_args->count;
    int r = t_args->r;
    pthread_mutex_t* read_lock = t_args->read_lock;  // Χρησιμοποιούμε το engine_lock

    int i;
    double cost;
    long long start, end;
    Variant sk, sv;
    DB* db = t_args->db;  // Λαμβάνουμε τον pointer στη βάση από το ThreadArgs

    char key[KSIZE + 1];
    char val[VSIZE + 1];
    char sbuf[1024];

    memset(key, 0, KSIZE + 1);
    memset(val, 0, VSIZE + 1);
    memset(sbuf, 0, 1024);

    // Δεν καλούμε πάλι db_open, γιατί DB* db έχει περαστεί μέσω του ThreadArgs

    start = get_ustime_nanosec();
    for (i = 0; i < count; i++) {
        if (r)
            _random_key(key, KSIZE);
        else
            snprintf(key, KSIZE, "key-%d", (int)(count*t_args->thread_num)+i);
        fprintf(stderr, "%d adding %s\n", i, key);
        snprintf(val, VSIZE, "val-%d", i);

        sk.length = KSIZE;
        sk.mem = key;
        sv.length = VSIZE;
        sv.mem = val;

        // Debug παραδείγματα για mutex (μπορείς να τα αφαιρέσεις μετά το debugging)
        //fprintf(stderr, "Mutex address (from args) in _write_test: %p\n", (void*)read_lock);
        pthread_mutex_lock(read_lock); 
        // (Πρόσθεσε debug log αν επιθυμείς)
        db_add(db, &sk, &sv);
        pthread_mutex_unlock(read_lock);
        // (Πρόσθεσε debug log αν επιθυμείς)

        if ((i % 10000) == 0) {
            fprintf(stderr, "random write finished %d ops%30s\r", i, "");
            fflush(stderr);
        }
    }
    end = get_ustime_nanosec();
    cost = end - start;
    printf(LINE);
    printf("|Random-Write\t(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n",
           count, cost/count, count/cost, cost);
    return;
} 

void _read_test(void* args) {
    ThreadArgs* t_args = (ThreadArgs*)args; // Μετατροπή των παραμέτρων
    long int count = t_args->count;
    int r = t_args->r;
    // Για τη read_test, χρησιμοποιούμε το mutex που αφορά το sst_merge, όπως πριν:
    pthread_mutex_t* cv_lock = &(t_args->db->sst->cv_lock);
    int i;
    int ret;
    int found = 0;
    double cost;
    long long start, end;
    Variant sk, sv;
    DB* db = t_args->db;  // Χρησιμοποιούμε τον ίδιο pointer στη βάση
    char key[KSIZE + 1];

    start = get_ustime_nanosec();
    for (i = 0; i < count; i++) {
        memset(key, 0, KSIZE+1);
        if (r)
            _random_key(key, KSIZE);
        else
            snprintf(key, KSIZE, "key-%d", (int)(count*t_args->thread_num)+i);
        fprintf(stderr, "%d searching %s\n", i, key);
        sk.length = KSIZE;
        sk.mem = key;

        //fprintf(stderr, "About to lock sst->cv_lock, address: %p\n", (void*)cv_lock);
        //pthread_mutex_lock(cv_lock);
        //fprintf(stderr, "Locked sst->cv_lock\n");
        ret = db_get(db, &sk, &sv);
        pthread_mutex_unlock(cv_lock);
        //fprintf(stderr, "Unlocked sst->cv_lock\n");

        if (ret) {
            found++;
        } else {
            //INFO("not found key#%s", sk.mem);
        }
        if ((i % 10000) == 0) {
            fprintf(stderr, "random read finished %d ops%30s\r", i, "");
            fflush(stderr);
        }
    }
	//get_ustime_nanosec(void)
    end = get_ustime_nanosec();
    cost = end - start;
    printf(LINE);
    printf("|Random-Read\t(done:%ld, found:%d): %.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec)\n",
           count, found, cost/count, count/cost, cost);
    return;
}


