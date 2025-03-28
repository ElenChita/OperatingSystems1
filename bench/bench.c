#include "bench.h"
#include <pthread.h>
#include "kiwi.h"
#include "../engine/db.h"
#include "../engine/sst.h"

#define N 3

#define DATAS ("testdb")

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n", 
			KSIZE);
	printf("Values: \t%d bytes each\n", 
			VSIZE);
	printf("Entries:\t%d\n", 
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s", 
			(char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s", 
				num_cpus, 
				cpu_type);

		printf("CPUCache:\t%s\n", 
				cache_size);
	}
}

/*int main(int argc,char** argv)
{
	long int count;
	//pthread_t thread1, thread2;  //make 2 threads
	//pthread_mutex_t read_lock;  //initialize mutex
	//pthread_mutex_init(&read_lock, NULL);  //create mutex

    int num_thread;
    struct args givergs1, giveargs2;
    long int percentage_read, percentage_write;
    long long start, end;
    double cost;
    double final_time = 0;
    long long readwrite_time = 0;

    void* void_write_test(void* arg) {
        struct args* arguments = (struct args *) arg;
        _write_test(arguments->count, arguments->r, arguments->thread_num);
        return 0;
    }

    void* void_read_test(void* arg) {
        struct args* arguments = (struct args *) arg;
        _write_test(arguments->count, arguments->r, arguments->thread_num);
        return 0;
    }

    void* void_writenew_test(void* arg) {
        struct args* arguments = (struct args *) arg;
        _writenew_test(arguments->count, arguments->r, arguments->thread_num);
        return 0;
    }

    void* void_readnew_test(void* arg) {
        struct args* arguments = (struct args *) arg;
        _readnew_test(arguments->count, arguments->r, arguments->thread_num);
        return 0;
    }

//thread1 kai 2 (pthread_thread_t) kai read_lock (pthread_mutex_t) diaforetikoi tipoi
	srand(time(NULL));
	if (argc < 6) {
		fprintf(stderr,"Usage: db-bench <write | read> <count> <numOfThreads> <percentage_read> <percentage_write>\n");
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		int r = 0;

		count = atoi(argv[2]);

        num_thread = atoi(argv[3]);

		_print_header(count);
		_print_environment();
		if (argc == 5)
			r = 1;
		

		//_write_test(count, r);

		    // Δημιουργία του struct ThreadArgs για παραμέτρους των threads
		//ThreadArgs args1 = {count, &r, &read_lock};
		//ThreadArgs args2 = {count, &r, &read_lock};
		
		// Άνοιγμα βάσης δεδομένων και δέσμευση στο struct
		//DB* db = db_open(DATAS);  // Άνοιγμα βάσης δεδομένων
		//args1.db = db;  // Δέσμευση της βάσης στο struct για το write thread
		//args2.db = db;

        open_db();
        start = get_ustime_sec();
        givergs1.r = r;
        givergs1.count = count;
        givergs1.thread_num = num_thread;
        pthread_t* threads = (pthread_t*) calloc(num_thread,sizeof(pthread_t));
        for (int i = 0; i < num_thread; i++){
            pthread_create(&threads[i], NULL, (void*) void_write_test, (void*)&givergs1);
        }
        for (int i = 0; i < num_thread; i++) {
            pthread_join(threads[i], NULL);
        }

        free(threads);
        close_db();

        end = get_ustime_sec();
        cost = end - start;

        final_time = final_time+cost;

        print(LINE);
        printf("|Random-Write    %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec)\n",
            (double)(final_time/count),
            (double)(count/final_time),
            final_time);
	} else if (strcmp(argv[1], "read") == 0) {
		int r = 0;

		count = atoi(argv[2]);

        num_thread = atoi(argv[3]);
        pthread_t* threads = (pthread_t*) calloc(num_thread, sizeof(pthread_t));

		_print_header(count);
		_print_environment();
		if (argc == 5)
			r = 1;
		
		//_read_test(count, r);
        open_db();
        start = get_ustime_sec();
        givergs1.r = r;
        givergs1.count = count;
        givergs1.thread_num = num_thread;

        for (int i = 0; i < num_thread; i++){
            pthread_create(&threads[i], NULL, (void*) void_write_test, (void*)&givergs1);
        }
        for (int i = 0; i < num_thread; i++) {
            pthread_join(threads[i], NULL);
        }

        free(threads);
        close_db();

        end = get_ustime_sec();
        cost = end - start;

        final_time = final_time+cost;

        print(LINE);
        printf("|Random-Write    %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec)\n",
            (double)(final_time/count),
            (double)(count/final_time),
            final_time);
    } else_if (strcmp(argv[1], "readwrite") == 0) {
        int r = 0;
        count = atoi(argv[2]);
        num_thread = atoi(argv[3]);
        pthread_t* threads = (pthread_t*) calloc(num_thread, sizeof(pthread_t));

        _print_header(count);
		_print_environment();

        if (argc == 7){
            r = 1;
        }

        open_db();
        start = get_ustime_sec();

        percentage_read = atoi(argv[4]);
        percentage_write = atoi(argv[5]);

        if (percentage_read + percentage_write = 100) {
            fprintf(stderr, "percs must be equal to 100\n");
            exit(1);
        }

        givergs1.r = r;

        if (percentage_read <= 20 && count < 10) {
            giveargs1.count = 1;
        } else {
            givergs1.count = ((count*percentage_read)/100);
        }

        if (percentage_read <= 20 && num_thread < 10) {
            giveargs1.thread_num = 1;
        } else {
            givergs1.thread_num = ((num_thread*percentage_read)/100);
        }

        giveargs2.r = r;

        if (percentage_write <= 20 && count < 10) {
            giveargs2.count = 1;
        } else {
            giveargs2.count = ((count*percentage_write)/100);
        }

        if (percentage_write <= 20 && num_thread < 10) {
            giveargs2.thread_num = 1;
        } else {
            giveargs2.thread_num = ((num_thread*percentage_write)/100);
        }

        for (int i = 0; i < num_thread*percentage_read/100; i++){
            pthread_create(&threads[i], NULL, (void*) void_readnew_test, (void*)&givergs1);
        }
        for (int i = 0; i < num_thread*percentage_write/100; i++){
            pthread_create(&threads[i], NULL, (void*) void_writenew_test, (void*)&giveargs2);
        }
        for (int i = 0; i < num_thread; i++) {
            pthread_join(threads[i], NULL);
        }

        free(threads);
        close_db();

        end = get_ustime_sec();
        cost = end - start;

        readwrite_time = readwrite_time+cost;
        printf(LINE);
        printf("|Random-Read-Write  %.6lld nanosec?op; %.1lld readwrites /sec(estimated); cost:%.3lld(nanosec)\n", (long long)(readwrite_time/count),
                (long long)(count/ readwrite_time), readwrite_time);

        printf("Writes: (done:%ld): %.6lld nanosec/op; %.1lld writes/nanosec(estimated); cost:%.3lld(sec)\n",
        (count*percentage_write/100), (long long)(readwrite_time/count),
        (long long)(count/readwrite_time), (long long)return_reading_time());

	} else {
		fprintf(stderr,"Usage: db-bench <write | read> <count> <random>\n");
		exit(1);
	}

	//Create 2 threads
	//pthread_create(&thread1, NULL, _write_test, (void*)&read_lock);
	//pthread_create(&thread2, NULL, _read_test, (void*)&read_lock);

	//Await for the threads to terminate
	//pthread_join(thread1, NULL);
	//pthread_join(thread2, NULL);

	//destroy mutex
	//pthread_mutex_destroy(&read_lock);
	
	//1o vhma
	//pthread_create(&thread1, NULL, _write_test, count,r)  //ta count kai r einai ta orismmata apo th write_test
	//
	//pthread_create(&thread2, NULL, _read_test, 100,r) 

	// na valw kai join gia na trexei swsta
	//isos se for gia perissotera 
	//ayta trexoyn parallhla

	//syxronismos 
	//pthread_mutex_init(read_lock)
	//pthread_create(&thread1, NULL, _write_test, count,r,read_lock)  //ta count kai r einai ta orismmata apo th write_test
	//
	//pthread_create(&thread2, NULL, _read_test, 100,r, read_lock) 

	//3o vhma sixrfonismos me to thread tou sstmerge (db->sst->cv_lock)
	return 1;
} */

/*int main(int argc, char** argv) {
    long int count; 
    pthread_t thread1, thread2,thread_merge;  // Δημιουργία 3 threads
    pthread_mutex_t read_lock;  // Αρχικοποίηση mutex
    pthread_mutex_init(&read_lock, NULL);  // Δημιουργία του mutex

    srand(time(NULL));  // Αρχικοποίηση για random keys

    if (argc < 3) {
        fprintf(stderr, "Usage: db-bench <write | read> <count>\n");
        exit(1);
    }

    int r = 0;  // Αντιπροσωπεύει εάν τα κλειδιά είναι τυχαία (1) ή σταθερά (0)
    count = atoi(argv[2]);  // Ανάκτηση αριθμού δεδομένων από τα ορίσματα

    // Τύπωσε πληροφορίες περιβάλλοντος
    _print_header(count);
    _print_environment();

    // Έλεγχος αν η επιλογή είναι για τυχαία κλειδιά
    if (argc == 4) {
        r = 1;  // Ορισμός ότι τα κλειδιά θα είναι τυχαία
    }

    // Δημιουργία παραμέτρων για τα threads
    ThreadArgs args1 = {count, &r, &read_lock}; // Ορίσματα για το write thread
    ThreadArgs args2 = {count, &r, &read_lock}; // Ορίσματα για το read thread

    if (strcmp(argv[1], "write") == 0) {
        // Εκτέλεση μόνο του write test
        pthread_create(&thread1, NULL, (void* (*)(void*))_write_test, &args1);
        pthread_join(thread1, NULL);  // Περιμένετε να ολοκληρωθεί
    } else if (strcmp(argv[1], "read") == 0) {
        // Εκτέλεση μόνο του read test
        pthread_create(&thread2, NULL, (void* (*)(void*))_read_test, &args2);
        pthread_join(thread2, NULL);  // Περιμένετε να ολοκληρωθεί
    } else if  (strcmp(argv[1], "both") == 0) {
		// Create threads for write, read, and merge
        pthread_create(&thread_merge, NULL, (void* (*)(void*))sst_merge, (void*)db->sst); // Simulate merge thread
        pthread_create(&thread1, NULL, (void* (*)(void*))_write_test, &args1);
        pthread_create(&thread2, NULL, (void* (*)(void*))_read_test, &args2);

        // Wait for threads to complete
        //pthread_join(thread_merge, NULL);
        pthread_join(thread1, NULL);
        pthread_join(thread2, NULL);
	}else {
        fprintf(stderr, "Usage: db-bench <write | read> <count>\n");
        exit(1);
    }

    // Καταστροφή mutex
    //pthread_mutex_destroy(&read_lock);

    //return 0;
//} */

int main(int argc, char** argv) {
    long int count;
    pthread_t write_threads[N], read_threads[N], merge_thread;
    pthread_mutex_t read_lock;
    pthread_mutex_init(&read_lock, NULL);

    srand(time(NULL));

    if(argc < 3) {
        fprintf(stderr, "Usage: db-bench <write | read | both> <count> <random>\n");
        exit(1);
    }

    int r = 0;
    count = atoi(argv[2]);

    _print_header(count);
    _print_environment();

    if(argc >= 4)
        r = 1;

    // Δημιουργία δομής παραμέτρων για τα νήματα
    ThreadArgs argswrite[N];
    ThreadArgs argsread[N];
    DB* db = db_open(DATAS);

    for(int i = 0; i < N; i++) {
        argswrite[i] = (ThreadArgs){(int)((0.2*count)/N), r, &read_lock, db, i};
        argsread[i] = (ThreadArgs){(int)((0.8*count)/N), r, &read_lock, db, i};
    }
    if(strcmp(argv[1], "write") == 0) {
        // Δημιουργία N threads για write
        for (int i = 0; i < N; i++) {
            pthread_create(&write_threads[i], NULL, (void*)_write_test, (void*)&argswrite[i]);
        }
        for (int i = 0; i < N; i++) {
            pthread_join(write_threads[i], NULL);
        }
    }
    else if(strcmp(argv[1], "read") == 0) {
        // Δημιουργία N threads για read
        for (int i = 0; i < N; i++) {
            pthread_create(&read_threads[i], NULL, (void*)_read_test, (void*)&argsread[i]);
        }
        for (int i = 0; i < N; i++) {
            pthread_join(read_threads[i], NULL);
        }
    }
    else if(strcmp(argv[1], "both") == 0) {
        // Δημιουργία του merge thread (π.χ., λύση για συγχώνευση στην background)
        //pthread_create(&merge_thread, NULL, (void* (*)(void*))sst_merge, (void*)&read_lock);
        // Δημιουργία N threads για write
        for (int i = 0; i < N; i++) {
            pthread_create(&write_threads[i], NULL, (void*)_write_test, (void*)&argswrite[i]);
        }
        // Δημιουργία N threads για read
        for (int i = 0; i < N; i++) {
            pthread_create(&read_threads[i], NULL, (void*)_read_test, (void*)&argsread[i]);
        }
        // Αναμονή για όλα τα threads
        //pthread_join(merge_thread, NULL);
        for (int i = 0; i < N; i++) {
            pthread_join(write_threads[i], NULL);
            pthread_join(read_threads[i], NULL);
        }
    }
    else {
        fprintf(stderr, "Usage: db-bench <write | read | both> <count> <random>\n");
    }
    db_close(db);
    pthread_mutex_destroy(&read_lock);

    return 0;
} 

/*int main(int argc, char** argv) {
    long int count;
    pthread_t thread_write, thread_read, thread_merge;
    pthread_mutex_t engine_lock;
    pthread_mutex_init(&engine_lock, NULL);

    srand(time(NULL));

    if (argc < 3) {
        fprintf(stderr, "Usage: db-bench <write | read | both> <count> <random>\n");
        exit(1);
    }

    int r = 0;
    count = atoi(argv[2]);
    _print_header(count);
    _print_environment();
    if (argc == 4) {
        r = 1;
    }

    // Ανοίγουμε τη βάση μία φορά
    DB* db = db_open(DATAS);

    // Δημιουργία του struct παραμέτρων για τα threads, περιλαμβάνοντας το DB* pointer
    ThreadArgs args_write = { count, &r, &engine_lock, db };
    ThreadArgs args_read  = { count, &r, &engine_lock, db };

    if (strcmp(argv[1], "write") == 0) {
        pthread_create(&thread_write, NULL, (void* (*)(void*))_write_test, &args_write);
        pthread_join(thread_write, NULL);
    } else if (strcmp(argv[1], "read") == 0) {
        pthread_create(&thread_read, NULL, (void* (*)(void*))_read_test, &args_read);
        pthread_join(thread_read, NULL);
    } else if (strcmp(argv[1], "both") == 0) {
        // Δημιουργία του merge thread, περνώντας έναν έγκυρο pointer στο SST (db->sst)
        //pthread_create(&thread_merge, NULL, (void* (*)(void*))sst_merge, (void*)db->sst);
        pthread_create(&thread_write, NULL, (void* (*)(void*))_write_test, &args_write);
        pthread_create(&thread_read, NULL, (void* (*)(void*))_read_test, &args_read);
        //pthread_join(thread_merge, NULL);
        pthread_join(thread_write, NULL);
        pthread_join(thread_read, NULL);
    } else {
        fprintf(stderr, "Usage: db-bench <write | read | both> <count> <random>\n");
        exit(1);
    }

    db_close(db);
    pthread_mutex_destroy(&engine_lock);

    return 0; 
} */
