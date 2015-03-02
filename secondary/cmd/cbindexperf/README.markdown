cbindexperf
===========

Bulk scan request perf reporting tool for secondary indexes

## Examples

    $ cbindexperf -cluster 127.0.0.1:9000 -configfile config.json -resultfile result.json

    $ cat config.json
    {
       "ScanSpecs" : [
          {
             "Type" : "All",
             "Id" : 1,
             "Index" : "first_name",
             "Repeat" : 0,
             "Limit" : 100,
             "Bucket" : "default"
          },
          {
             "Limit" : 0,
             "Low" : [
                "Aaron"
             ],
             "Repeat" : 0,
             "Type" : "Range",
             "Bucket" : "default",
             "High" : [
                "Z"
             ],
             "Id" : 2,
             "Inclusion" : 3,
             "Index" : "first_name"
          },
          {
             "Repeat" : 0,
             "Limit" : 0,
             "Type" : "Lookup",
             "Bucket" : "default",
             "Lookups" : [
                [
                   "Aaron"
                ],
                [
                   "Adela"
                ]
             ],
             "Index" : "first_name",
             "Id" : 2,
             "Inclusion" : 3
          }
       ]
    }


    $ cat result.json
    {
       "ScanResults" : [
          {
             "Rows" : 100,
             "Duration" : 18705899,
             "Error" : "",
             "Id" : 1
          },
          {
             "Duration" : 28055551,
             "Rows" : 998,
             "Error" : "",
             "Id" : 2
          },
          {
             "Rows" : 1000,
             "Duration" : 27615818,
             "Id" : 2,
             "Error" : ""
          }
       ]
    }

