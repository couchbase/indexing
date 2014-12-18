## Examples

# Usage:
    Usage: ./querycmd -type scanAll -index idx1 -bucket default
      -bucket="default": Bucket name
      -buffersz=0: Rows buffer size per internal message
      -equal="": Range: [key]
      -fields="": Comma separated on-index fields
      -high="": Range: [high]
      -incl=0: Range: 0|1|2|3
      -index="": Index name
      -instanceid="": Index instanceId
      -limit=10: Row limit
      -low="": Range: [low]
      -primary=false: Is primary index
      -server="localhost:9000": Cluster server address
      -type="scanAll": Index command (scan|stats|scanAll|create|drop|list)

# Scan
    $ querycmd -type=scanAll -bucket default -index abcd
    $ querycmd -type=scanAll -index abcd -limit 0
    $ querycmd -type=scan -index state -low='["Ar"]' -high='["Co"]' -buffersz=300
    $ querycmd -type=scan -index name_state_age -low='["Ar"]' -high='["Arlette", "N"]'
    $ querycmd -type scan -index '#primary' -equal='["Adena_54605074"]'

# Create
    $ querycmd -type create -bucket default -index first_name -fields=first_name,last_name
    $ querycmd -type create -bucket default -primary=true

# Drop
    $ querycmd -type drop -instanceid 1234

# List
    $ querycmd -type list

