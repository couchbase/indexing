## Index Status

### Status can be obtained from:

    $ curl localhost:9102/getIndexStatus
    {
      "code": "success",
      "status": [
        {
            "defnId": 15184130463866081000,
            "name": "breweries",
            "bucket": "beer-sample",
            "secExprs": [ "`name`" ],
            "status": "Ready",
            "hosts": [ "127.0.0.1:8091" ]
        },
        {
        
            "defnId": 16833079877526070000,
            "name": "#primary",
            "bucket": "beer-sample",
            "isPrimary": true,
            "status": "Ready",
            "hosts": [ "127.0.0.1:8091" ]
        },
      ]
    }

* defnId     = Internal id of the index
* name       = User supplied name
* bucket     = The bucket the index is defined on
* secExpres  = Expressions defining the index fields (ordered)
* status     = Index status (Not Available / Created / Build / Ready)
* isPrimary  = Indicates if index is primary (index on document key)
* host       = Indexer node serving this index

              
## Index Statistics

### Stats can be obtained from:

        $ curl localhost:9102/stats
        {
           "default:num_mutations_queued" : 0,
           "default:first_name:scan_bytes_read" : 0,
           "default:first_name:avg_scan_latency" : 0,
           "default:first_name:total_scan_duration" : 0,
           "default:first_name:get_bytes" : 0,
           "default:first_name:num_docs_queued" : 0,
           "default:first_name:num_compactions" : 0,
           "memory_quota" : 268435456,
           "needs_restart" : false,
           "memory_used" : 823296,
           "default:first_name:num_docs_pending" : 0,
           "default:first_name:scan_wait_duration" : 0,
           "default:first_name:delete_bytes" : 0,
           "default:first_name:flush_queue_size" : 0,
           "default:first_name:num_flush_queued" : 0,
           "default:first_name:num_rows_returned" : 0,
           "default:first_name:avg_ts_interval" : 0,
           "default:mutation_queue_size" : 0,
           "default:first_name:num_snapshots" : 0,
           "default:first_name:disk_size" : 20480,
           "default:first_name:insert_bytes" : 0,
           "default:first_name:items_count" : 0,
           "default:first_name:data_size" : 0,
           "num_connections" : 0,
           "default:first_name:num_commits" : 0,
           "default:first_name:avg_scan_wait_latency" : 0,
           "default:first_name:build_progress" : 100,
           "default:first_name:num_requests" : 0,
           "default:first_name:num_docs_indexed" : 0
        }

### Per Index Statistics

For each index following set of stats will be published with bucket:index\_name:stats\_name format:

##### "default:first\_name32:num\_requests" : 4,
Number of scan requests served by the indexer

##### "default:first\_name:flush\_queue\_size" : 0,
Number of items currently in the flusher queue

##### "default:first\_name:num\_flush\_queued" : 0,
Number of items inserted into flusher queue so far

##### "default:first\_name:avg\_scan\_latency" : 0,
Mean scan latency

##### "default:first\_name:avg\_scan\_wait\_latency" : 0,
Mean latency incurred in waiting for read snapshot

##### "default:first\_name:num\_compactions" : 0,
Number of compactions performed the indexer

##### "default:first\_name:num\_compactions" : 0,
Number of database commits performed by the indexer

##### "default:first\_name:num\_snapshots" : 0,
Number of storage snapshots generated

##### "default:first\_name:avg\_ts\_interval" : 0,
Average interval for snapshot generation

##### "default:first\_name32:num\_docs\_pending" : 0,
Number of documents yet to be received from KV by
the indexer.

##### "default:first\_name32:num\_rows\_returned" : 60,
Total number rows returned so far by the indexer


##### "default:first\_name32:num\_docs\_queued" : 0,
Number of documents queued in indexer, but not indexed yet


##### "default:first\_name32:num\_docs\_indexed" : 62308,
Total number of docs indexed so far by the indexer from the
time indexer has restarted.


##### "default:first\_name32:num\_items\_flushed" : 62308,
Total number of item mutations applied in the index. For
array indexes, one document mutation may lead to multiple
items to be updated.


##### "default:first\_name32:num\_docs\_processed" : 62308,
Total number of docs processed by the indexer with respect
to the bucket. This stat is computed based on latest
kv sequence numbers seen by the indexer for all vbuckets.


##### "default:first\_name32:disk\_size" : 13127680,
Total disk file size consumed by the index


##### "default:first\_name32:data\_size" : 13009634
Actual data size consumed by the index


##### "default:first\_name32:scan\_bytes\_read" : 4096
Total decoded bytes read from the index for serving scan requests


##### "default:first\_name32:items\_count" : 12000
Current total number of rows in the index. This is an approximate count and may be incorrect.

##### "default:first\_name7:build\_progress: 93
Initial build progress for the index. When index has completed initial build,
this stat will disappear from the stats.


##### "default:first\_name7:get\_bytes" : 0,
##### "default:first\_name7:delete\_bytes" : 0,
##### "default:first\_name7:insert\_bytes" : 1271211,
Bytes requested to forestdb operations (insert, delete, get)

### Indexer wide statistics

##### "num\_connections" : 0,
Current num connections used by indexer


##### "needs\_restart" : false,
Whether indexer needs restart to apply current mem\_quota settings


##### "default:first\_name7:scan\_wait\_duration" : 331286,
Total time taken (ns) for readable snapshot to be available for consistent query (similar to stale=false)


##### "default:first\_name7:total\_scan\_duration" : 1693478681,
Time taken for index scans

##### "default:num\_mutations\_queued" : 1000,
Total mutations queued so far

#### "default:mutation\_queue\_size": 10,
Current number of items in the mutations queue

#### "memory\_quota" : 256000,
Memory quota set for the indexer

#### "memory\_used" : 1000,
Memory used by the storage engine


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

