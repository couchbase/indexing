#! /usr/bin/env bash

URL=http://localhost:9499/query
HEADER='-H Content-Type:text/plain'

run_query(){
    echo $1
    curl -s -X POST $HEADER --data "$1" $URL | $2
    echo
}

# cleanup
run_query 'DROP PRIMARY INDEX ON default:default'       'grep "error\|cause\|msg\|success" -i'
run_query 'DROP INDEX default:default.idx1'    'grep "error\|cause\|msg\|success" -i'
run_query 'DROP PRIMARY INDEX ON default:`beer-sample`' 'grep "error\|cause\|msg\|success" -i'
run_query 'DROP INDEX default:`beer-sample`.idx2'    'grep "error\|cause\|msg\|success" -i'

# create-index
run_query 'CREATE PRIMARY INDEX ON default:default USING GSI'       'grep "error\|cause\|msg\|success" -i'
run_query 'CREATE INDEX idx1 ON default:default(age) USING GSI'       'grep "error\|cause\|msg\|success" -i'
run_query 'CREATE PRIMARY INDEX ON default:`beer-sample` USING GSI' 'grep "error\|cause\|msg\|success" -i'
run_query 'CREATE INDEX idx2 ON default:`beer-sample`(abv) USING GSI' 'grep "error\|cause\|msg\|success" -i'

SLEEP=5
echo "Sleeping for $SLEEP seconds ..."
#sleep $SLEEP

# query
run_query 'SELECT \* FROM system:`indexes`'                                'cat'
run_query 'SELECT \* FROM default:default LIMIT 2'                       'cat'
run_query 'SELECT count(*) FROM default:default WHERE age > 10'          'cat'
run_query 'SELECT count(*) FROM default:default WHERE gender = "female"' 'cat'
run_query 'SELECT \* FROM default:`beer-sample` LIMIT 2'                 'cat'
run_query 'SELECT count(*) FROM default:`beer-sample` WHERE abv > 10'    'cat'
run_query 'SELECT count(*) FROM default:`beer-sample` WHERE type = "beer"' 'cat'
