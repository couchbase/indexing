## Index Statistics

### Stats can be obtained from:

        $ curl localhost:9102/stats
        {
           "default:first_name7:num_requests" : "3",
           "default:first_name7:items_count" : "10000",
           "default:first_name7:insert_bytes" : "1271211",
           "default:first_name7:disk_size" : "3563520",
           "default:first_name7:delete_bytes" : "0",
           "default:first_name7:total_scan_duration" : "1693478681",
           "default:first_name7:num_docs_queued" : "0",
           "default:first_name7:scan_bytes_read" : "799260",
           "default:first_name7:scan_wait_duration" : "331286",
           "default:first_name7:num_docs_indexed" : "10000",
           "default:first_name7:num_docs_pending" : "0",
           "needs_restart" : "false",
           "default:first_name7:num_rows_returned" : "30000",
           "num_connections" : "0",
           "default:first_name7:get_bytes" : "3869880"
        }


### Per Index Statistics

For each index following set of stats will be published with bucket:index\_name:stats\_name format:

##### "default:first\_name32:num\_requests" : "4",
Number of scan requests served by the indexer


##### "default:first\_name32:num\_docs\_pending" : "0",
Number of documents pending to be indexed


##### "default:first\_name32:num\_rows\_returned" : "60",
Total number rows returned so far by the indexer


##### "default:first\_name32:num\_docs\_queued" : "0",
Number of documents queued in indexer, but not indexed yet


##### "default:first\_name32:num\_docs\_indexed" : "62308",
Total number of docs indexed so far by the indexer


##### "default:first\_name32:disk\_size" : "13127680",
Total disk file size consumed by the index


##### "default:first\_name32:data\_size" : "13009634"
Actual data size consumed by the index


##### "default:first\_name32:scan\_bytes\_read" : "4096"
Total decoded bytes read from the index for serving scan requests


##### "default:first\_name32:items\_count" : "12000"
Current total number of rows in the index


##### "default:first\_name7:get\_bytes" : "0",
##### "default:first\_name7:delete\_bytes" : "0",
##### "default:first\_name7:insert\_bytes" : "1271211",
Bytes requested to forestdb operations (insert, delete, get)

### Indexer wide statistics

##### "num\_connections" : "0",
Current num connections used by indexer


##### "needs\_restart" : "false",
Whether indexer needs restart to apply current mem\_quota settings


##### "default:first\_name7:scan\_wait\_duration" : "331286",
Total time taken (ns) for readable snapshot to be available for consistent query (similar to stale=false)


##### "default:first\_name7:total\_scan\_duration" : "1693478681",
Time taken for index scans


## Indexer settings

All normal setings get/set should go through [metakv](https://github.com/couchbase/cbauth/tree/master/metakv).

Indexer will be able to dynamically update all the settings during runtime except memory\_quota.

#### Snapshot intervals (ms)

##### "settings.inmemory\_snapshot.interval" : 204800
##### "settings.persisted\_snapshot.interval" : 204800

Persisted snapshot intervals should be multiple of inmemory snapshot interval

Minimum: 100ms

#### Compaction settings


##### "settings.compaction.min\_size" : 1048576,

Minimum file size required to trigger compaction

Minimum: 50MB


##### "settings.compaction.check\_period" : 60,

Intervals to check if compaction threshold has reached

Minimum: 60s


##### "settings.compaction.min\_frag" : 30,

Minimum fragmentation percentage to trigger compaction

Minimum: 5%

#### System usage settings


##### "settings.compaction.interval" : "00:00,00:00",

Time interval during which compaction should be allowed to run (start\_hr:start\_min, end\_hr:end\_min)

Default value "00:00,00:00" means anytime.


##### "settings.max\_cpu\_percent" : 300,

Minimum: 100%

If it is set to 0, indexer will use all cpus


##### "settings.memory\_quota" : 0,

Indexer memory quota for forestdb.

Minimum: 0 (Disabled)


##### "settings.recovery.max\_rollbacks" : 5

Maximum number of snapshots to be kept even after compaction on forestdb

Minimum: 1

### Debugging Settings
All normal get/set should go through [metakv](https://github.com/couchbase/cbauth/tree/master/metakv).

For reading settings for debugging:

        $ curl localhost:9102/settings
        {
           "settings:compaction:min_frag" : 30,
           "settings:persisted_snapshot:interval" : 200,
           "settings:inmemory_snapshot:interval" : 200,
           "settings:max_cpu_percent" : 100,
           "settings:compaction:check_period" : 60,
           "settings:compaction:min_size" : 1048576,
           "settings:memory_quota" : 0,
           "settings:recovery:max_rollbacks" : 5,
           "settings:compaction:interval" : "00:00,00:00"
        }
        
To writing settings for debugging, read settings into a file, modify and

        curl localhost:9102/settings -d @settings:json


### Manual compaction
        $ curl localhost:9102/triggerCompaction

