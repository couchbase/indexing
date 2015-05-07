  // +build ignore

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <stdint.h>
 #include <time.h>
 #include <sys/time.h>
 #if !defined(WIN32) && !defined(_WIN32)
 #include <unistd.h>
 #endif
 
 #include "libforestdb/forestdb.h"

 void do_test1()
 {

    printf("***** TEST1 SINGLE KV STORE 10M INSERT *****\n");

    fdb_file_handle *dbfile;
    fdb_kvs_handle *main, *back;

    char keybuf[256], bodybuf[256];
          
    fdb_config fconfig = fdb_get_default_config();
    fconfig.durability_opt = FDB_DRB_ASYNC;
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();

    fdb_open(&dbfile, "./ctest1", &fconfig);
    fdb_kvs_open(dbfile, &main, "main", &kvs_config);

    int i;
    int n = 10000000;

    char **keyarr = (char **)malloc(n * sizeof(char *)); 
    char **bodyarr = (char **)malloc(n * sizeof(char *)); 

    for (i=0;i<n;++i){
          keyarr[i] = (char *)malloc(25);
          bodyarr[i] = (char *)malloc(25);
          sprintf(keyarr[i], "key%d", i);
          sprintf(bodyarr[i], "body%d", i);
    }

    struct timeval ts_begin, ts_cur, ts_gap;
    gettimeofday(&ts_begin, NULL);
    for (i=0;i<n;++i){
          fdb_set_kv(main, (void*)keyarr[i], strlen(keyarr[i]), (void*)bodyarr[i], strlen(bodyarr[i]));
    }

    fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&ts_cur, NULL);
    timersub(&ts_cur, &ts_begin, &ts_gap);

    printf("***** RESULT : Docs Inserted %d Time Taken %d secs\n", n, (int)ts_gap.tv_sec);

    fdb_kvs_close(main);
    fdb_close(dbfile);
    fdb_shutdown();
    system("rm ./ctest1");
}

 void do_test2()
 {

    printf("***** TEST2 TWO KV STORES 5M INSERT EACH *****\n");

    fdb_file_handle *dbfile;
    fdb_kvs_handle *main, *back;

    char keybuf[256], bodybuf[256];
          
    fdb_config fconfig = fdb_get_default_config();
    fconfig.durability_opt = FDB_DRB_ASYNC;
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();

    fdb_open(&dbfile, "./ctest2", &fconfig);
    fdb_kvs_open(dbfile, &main, "main", &kvs_config);
    fdb_kvs_open(dbfile, &back, "back", &kvs_config);

    int i;
    int n = 5000000;

    char **keyarr = (char **)malloc(n * sizeof(char *)); 
    char **bodyarr = (char **)malloc(n * sizeof(char *)); 

    for (i=0;i<n;++i){
          keyarr[i] = (char *)malloc(25);
          bodyarr[i] = (char *)malloc(25);
          sprintf(keyarr[i], "key%d", i);
          sprintf(bodyarr[i], "body%d", i);
    }

    struct timeval ts_begin, ts_cur, ts_gap;
    gettimeofday(&ts_begin, NULL);
    for (i=0;i<n;++i){
          fdb_set_kv(back, (void*)keyarr[i], strlen(keyarr[i]), (void*)bodyarr[i], strlen(bodyarr[i]));
          fdb_set_kv(main, (void*)keyarr[i], strlen(keyarr[i]), NULL, 0);
    }

    fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&ts_cur, NULL);
    timersub(&ts_cur, &ts_begin, &ts_gap);
    printf("***** RESULT : Docs Inserted %d Time Taken %d secs\n", n, (int)ts_gap.tv_sec);

    fdb_kvs_close(main);
    fdb_kvs_close(back);
    fdb_close(dbfile);
    fdb_shutdown();
    system("rm ./ctest2");
}

 void do_test3()
 {

    printf("***** TEST3 TWO KV STORES 5M INSERT EACH WITH GET *****\n");

    fdb_file_handle *dbfile;
    fdb_kvs_handle *main, *back;

    char keybuf[256], bodybuf[256];
          
    fdb_config fconfig = fdb_get_default_config();
    fconfig.durability_opt = FDB_DRB_ASYNC;
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();

    fdb_open(&dbfile, "./ctest3", &fconfig);
    fdb_kvs_open(dbfile, &main, "main", &kvs_config);
    fdb_kvs_open(dbfile, &back, "back", &kvs_config);

    int i;
    int n = 5000000;
    size_t valuelen;
    void *value;

    char **keyarr = (char **)malloc(n * sizeof(char *)); 
    char **bodyarr = (char **)malloc(n * sizeof(char *)); 

    for (i=0;i<n;++i){
          keyarr[i] = (char *)malloc(25);
          bodyarr[i] = (char *)malloc(25);
          sprintf(keyarr[i], "key%d", i);
          sprintf(bodyarr[i], "body%d", i);
    }


    struct timeval ts_begin, ts_cur, ts_gap;
    gettimeofday(&ts_begin, NULL);
    for (i=0;i<n;++i){
          //fdb_doc_create(&doc[i], (void*)keybuf, strlen(keybuf),
           //      null, 0, (void*)bodybuf, strlen(bodybuf));
          //fdb_set(main, doc[i]);
          //fdb_doc_free(doc[i]);
          fdb_get_kv(back, (void*)keyarr[i], strlen(keyarr[i]), &value, &valuelen);
          fdb_set_kv(back, (void*)keyarr[i], strlen(keyarr[i]), (void*)bodyarr[i], strlen(bodyarr[i]));
          fdb_set_kv(main, (void*)keyarr[i], strlen(keyarr[i]), NULL, 0);
    }

    fdb_commit(dbfile, FDB_COMMIT_MANUAL_WAL_FLUSH);

    gettimeofday(&ts_cur, NULL);
    timersub(&ts_cur, &ts_begin, &ts_gap);
    printf("***** RESULT : Docs Inserted %d Time Taken %d secs\n", n, (int)ts_gap.tv_sec);

    fdb_kvs_close(main);
    fdb_kvs_close(back);
    fdb_close(dbfile);
    fdb_shutdown();
    system("rm ./ctest3");
}

int main(){
         do_test1();
         do_test2();
         do_test3();
}
