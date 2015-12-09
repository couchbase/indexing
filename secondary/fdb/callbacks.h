/* There is no guarantee that forestdb will use Go threads to invoke
 * callbacks. So it is essential all forestdb callbacks are in pure C.
 */
#include <libforestdb/forestdb.h>
void init_fdb_logging(fdb_kvs_handle* db, const char* name);
void init_fdb_breakpad(const char* diagdir);
