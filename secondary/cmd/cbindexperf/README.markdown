cbindexperf
===========

Bulk scan request perf reporting tool for secondary indexes

## Examples

    $ cbindexperf -cluster 127.0.0.1:9000 -configfile config.json -resultfile result.json

    $ cat config.json
    {
  "Concurrency": 1,
  "Clients": 1,
  "ScanSpecs": [
    {
      "Type": "All",
      "Id": 1,
      "Index": "first_name",
      "Repeat": 0,
      "Limit": 100,
      "Bucket": "default"
    },
    {
      "Limit": 0,
      "Low": [
        "Aaron"
      ],
      "Repeat": 0,
      "Type": "Range",
      "Bucket": "default",
      "High": [
        "Z"
      ],
      "Id": 2,
      "Inclusion": 3,
      "Index": "first_name"
    },
    {
      "Repeat": 0,
      "Limit": 0,
      "Type": "Lookup",
      "Bucket": "default",
      "Lookups": [
        [
          "Aaron"
        ],
        [
          "Adela"
        ]
      ],
      "Index": "first_name",
      "Id": 2,
      "Inclusion": 3
    },
    {
      "Limit": 0,
      "Repeat": 0,
      "Type": "MultiScan",
      "Bucket": "default",
      "Id": 4,
      "Index": "first_name",
      "Scans": [
        {
          "Filter": [
            {
              "Low": "Aaron",
              "High": "Scott",
              "Inclusion": 3
            },
            {
              "Low": 12,
              "High": 65,
              "Inclusion": 3
            }
          ]
        },
        {
          "Filter": [
            {
              "Low": "Steve",
              "High": "Hack",
              "Inclusion": 3
            },
            {
              "Low": 10,
              "High": 20,
              "Inclusion": 3
            }
          ]
        }
      ]
    },
    {
      "Limit": 0,
      "Repeat": 10,
      "Type": "Scan3",
      "Bucket": "default",
      "Id": 1,
      "Index": "ga_arr1",
      "Scans": [
        {
          "Filter": null
        }
      ],
      "GroupAggr": {
        "Name": "S4",
        "Group": [
          {
            "EntryKeyId": 6,
            "KeyPos": 2,
            "Expr": ""
          },
          {
            "EntryKeyId": 7,
            "KeyPos": 0,
            "Expr": ""
          }
        ],
        "Aggrs": [
          {
            "AggrFunc": 0,
            "EntryKeyId": 8,
            "KeyPos": 1,
            "Expr": "",
            "Distinct": false
          }
        ],
        "DependsOnIndexKeys": null,
        "IndexKeyNames": null
      },
      "IndexProjection": {
        "EntryKeys": [
          6,
          7,
          8
        ],
        "PrimaryKey": false
      }
    }
  ]
}


    $ cat result.json
    {
       "Rows" : 300000,
       "Duration" : 1.5,
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

