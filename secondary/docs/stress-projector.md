kv configuration:

|                |     small     |     medium    |     large     |
| -------------- | ------------- | ------------- | ------------- |
| No. of nodes   |      3        |      20       |      30       |
| Cores per node |      8        |      16       |      32       |
|    Network     |      1Gb      |       2Gb     |       3Gb     |
| No. of docs    |     50M       |     500M      |       5B      |
|     RAM        |     61GB      |      64GB     |     128GB     |
| Document size  |      1KB      |       1KB     |       1KB     |
| No. of buckets |      3        |       5       |       10      |

incremental build:

|   KV traffic   |     50k/s     |     150k/s    |     500k/s    |
|  Write-ratio   |     30%       |      30%      |      30%      |

* KV and indexer services managed on different set of nodes.
* load generated using separate set of nodes.
* All Storage is general purpose SSD.

2i configuration:

|                |     small     |     medium    |     large     |
| -------------- | ------------- | ------------- | ------------- |
| No. of nodes   |      1        |       5       |      10       |
| Cores per node |     32        |      16       |      32       |
|    Network     |      3Gb      |       2Gb     |       3Gb     |
| No. of buckets |      3        |       5       |      10       |
|     RAM        |    244GB      |     122GB     |     128GB     |
| No. of indexes |     15        |      25       |      50       |

* for all configuration 5 indexes defined in each bucket.
* All Storage is general purpose SSD.
* for `small` configuration 61GB of disk does not seem to be sufficient.
  40GB consumed for 10 indexes @60% and 5 indexes @0%, so bumping it to
  it to 122GB.
  even 122GB is not sufficient, bumping it to 32-c0re 244GB, after
  which index is getting built.

Projector observation:

|                 |     small     |     medium    |     large     |
| --------------- | ------------- | ------------- | ------------- |
|   Throughput    |               |               |               |
| CPU utilization |               |               |               |
| Memory Resident |               |               |               |
|  Dcp latency    |               |               |               |

* throughput is measured in mutations / second, for each mutation
  dataport shall carry N key-versions for N indexes hosted by the
  target indexer node.

small : (with 122GB indexer, 16 core)
* projector CPU utilizations is typically around 600%.
* projector RSS (min: 8.5MB, max: 3.4GB).
* projector is not saturating the DCP.
* in projector 65% of memory and 75% of objects are allocated by N1QL.
* dcp latency is on an average between 130uS to 180uS. It gives an idea
  on the averate rate in which the dcp socket is drained out - within
  a given snapshot.
* end-to-end latency is on an average around 2.5 second, since this
  was running in go1.4.1 we have to consider upto 40s gc-pause on the
  downstream.
* upsert rate is around 2.2M per bucket for every 5 minutes. Around
  7300 mutations per second per bucket.
* GC pause in projector is around 500mS when all its queues are
  filled up.
* indexer disk is periodically saturated (102%) at 118MB / second.
* indexer is always saturated @3200%.
* network throughput does not go beyong 220Mpbs / second on the indexer
  node.

small : (with 244GB indexer, 32 core)
* projector CPU utilizations is typically around 700%.
* projector RSS (min: 8.5MB, max: 3.4GB).
* projector is not saturating the DCP.
* in projector 65% of memory and 75% of objects are allocated by N1QL.
* dcp latency is on an average between 130uS to 180uS. It gives an idea
  on the averate rate in which the dcp socket is drained out - within
  a given snapshot.
* upsert rate is around 6k mutations / second / bucket, over 3 buckets
  and 15 indexes (5 for each bucket), amounts to 10uS wall-clock time
  per indexed key-version - but projector achieves this with 6-cores.
* GC pause in projector is around 500mS when all its queues are
  filled up.
* indexer disk is periodically saturated (102%) at 118MB / second.
* indexer is saturated @3000%.
* network throughput does not go beyong 220Mpbs / second on the indexer
  node.

Dataport observation:

|                 |     small     |     medium    |     large     |
| --------------- | ------------- | ------------- | ------------- |
|   Throughput    |               |               |               |
|    Bandwidth    |               |               |               |
