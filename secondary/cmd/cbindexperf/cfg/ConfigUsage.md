Usage for Config Files
======================

### idx_vector_{details}.json
* load data using randdocs tool
  > randdocs -config ./cmd/config.json -genVectors
* create index idx_vector something like below
  > CREATE INDEX idx_vector
    ON default(name, description VECTOR, city)
    WITH { "dimension":128, "description": "IVF256,PQ32x8", "similarity":"L2"};
* run cbindexperf command
  > cbindexperf -cluster 127.0.0.1:9001 -auth="Administrator:asdasd" -configfile ~/cbindexPerfConfig/scan_idx_vector_projection.json -resultfile result.json -logLevel info