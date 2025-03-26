#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include "kiwi.h"
#include <pthread.h>
#include "../engine/sst.h"

#define DATAS ("testdb")

void _write_test(void* args)
{
	ThreadArgs* t_args = (ThreadArgs*)args; // Pointer -> struct
    long int count = t_args->count;        // The following lines 
    int r = *((int*)t_args->r);            // just retrieve
    pthread_mutex_t* read_lock = t_args->read_lock; // the args given

	int i;
	double cost;
	long long start,end;
	Variant sk, sv;
	DB* db;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		//read_lock koino ara parametro sth write kai sth read
		pthread_mutex_lock(read_lock);  //lock for safe input of data

		  // Debug printing around the mutex critical section:
		  fprintf(stderr, "Mutex address (from args) in _write_test: %p\n", (void*)read_lock);
		  pthread_mutex_lock(read_lock);  // Lock for safe input of data
		  fprintf(stderr, "Locked mutex in _write_test: %p\n", (void*)read_lock);

		db_add(db, &sk, &sv);
		pthread_mutex_unlock(read_lock);  //unlock after the input of data

		pthread_mutex_unlock(read_lock);  // Unlock after input of data
        fprintf(stderr, "Unlocked mutex in _write_test: %p\n", (void*)read_lock);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _read_test(void* args)
{
	ThreadArgs* t_args = (ThreadArgs*)args; // Pointer -> struct
	//DB* db = t_args->db; 
    long int count = t_args->count;        // The following lines 
    int r = *((int*)t_args->r);            // just retrieve
    pthread_mutex_t* read_lock = t_args->read_lock; // the args given
	//pthread_mutex_t* cv_lock = db->sst->cv_lock; // Mutex για συγχρονισμό

	int i;
	int ret;
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	DB* db;
	char key[KSIZE + 1];

	db = db_open(DATAS);
	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;

		fprintf(stderr, "About to lock sst->cv_lock, address: %p\n", (void*)&(db->sst->cv_lock));
		pthread_mutex_lock(&(db->sst->cv_lock)); //sixronismos me sstmerge
		fprintf(stderr, "Locked sst->cv_lock\n");

		ret = db_get(db, &sk, &sv);
		
		pthread_mutex_unlock(&(db->sst->cv_lock));
		fprintf(stderr, "Unlocked sst->cv_lock\n");

		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	db_close(db);

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}
