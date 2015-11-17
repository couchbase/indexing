projector stress testing:

system is provisioned with following configuration for 2i-projector
stress testing.

|                |     small     |     medium    |     large     |
| -------------- | ------------- | ------------- | ------------- |
| No. of nodes   |      3        |      20       |      30       |
| Cores per node |      8        |      16       |      32       |
|    Network     |      1Gb      |       2Gb     |       3Gb     |
| No. of docs    |     50M       |     500M      |       5B      |
|     RAM        |     64GB      |      64GB     |     128GB     |
| Document size  |      1KB      |       1KB     |       1KB     |
| No. of buckets |      3        |       5       |       10      |
|   KV traffic   |     50k/s     |     150k/s    |     500k/s    |
|  Write-ratio   |     30%       |      30%      |      30%      |

* KV and indexer services managed on different set of nodes.
* load generated using separate set of nodes.

2i configuration:

|                |     small     |     medium    |     large     |
| -------------- | ------------- | ------------- | ------------- |
| No. of nodes   |      1        |       5       |      10       |
| Cores per node |      8        |      16       |      32       |
|    Network     |      1Gb      |       2Gb     |       3Gb     |
| No. of buckets |      1        |       5       |      10       |
|     RAM        |     64GB      |      64GB     |     128GB     |
| No. of indexes |      5        |      25       |      50       |

* for all configuration 5 indexes defined in each bucket.

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

Dataport observation:

|                 |     small     |     medium    |     large     |
| --------------- | ------------- | ------------- | ------------- |
|   Throughput    |               |               |               |
|    Bandwidth    |               |               |               |
