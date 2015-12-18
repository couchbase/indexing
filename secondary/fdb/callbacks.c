#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include <libforestdb/forestdb.h>
#include <breakpad_wrapper/breakpad_wrapper.h>
#include "callbacks.h"

void log_message(int code, const char* msg, void *ctx) {
   time_t now;
   time(&now);
   char ts[256];
   strftime(ts, sizeof ts, "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
   fflush(stdout);
   fprintf(stdout, "\n%s [Error] forestdb error - name:%s, code:%d, msg:%s\n", ts, (char*) ctx, code, msg);
   fflush(stdout);
}

void init_fdb_logging(fdb_kvs_handle* db, const char* name) {
   if (!db || !name) return;
   char* ctx = strdup(name); // leak, for safety
   fdb_set_log_callback(db, log_message, ctx);
}

void init_fdb_breakpad(const char* diagdir) {
   if (!diagdir || strlen(diagdir) < 1) return;
   breakpad_initialize(diagdir);
   uintptr_t bp_write = breakpad_get_write_minidump_addr();
   if (!bp_write) return;
   fdb_set_fatal_error_callback(bp_write);
}

