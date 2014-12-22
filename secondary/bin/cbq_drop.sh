#! /usr/bin/env bash

URL=http://localhost:8093/query
HEADER='-H Content-Type:text/plain'

run_query(){
    echo $1
    curl -s -X POST $HEADER --data "$1" $URL | $2
    echo
}

# cleanup
run_query 'DROP PRIMARY INDEX ON default:default'       'grep "error\|cause\|msg\|success" -i'
run_query 'DROP INDEX default:default.idx1'             'grep "error\|cause\|msg\|success" -i'
run_query 'DROP PRIMARY INDEX ON default:`beer-sample`' 'grep "error\|cause\|msg\|success" -i'
run_query 'DROP INDEX default:`beer-sample`.idx2'       'grep "error\|cause\|msg\|success" -i'

# create-index
run_query 'CREATE PRIMARY INDEX ON default:default USING GSI'         'grep "error\|cause\|msg\|success" -i'
run_query 'CREATE INDEX idx1 ON default:default(age) USING GSI'       'grep "error\|cause\|msg\|success" -i'
run_query 'CREATE PRIMARY INDEX ON default:`beer-sample` USING GSI'   'grep "error\|cause\|msg\|success" -i'
run_query 'CREATE INDEX idx2 ON default:`beer-sample`(abv) USING GSI' 'grep "error\|cause\|msg\|success" -i'

# drop
run_query 'DROP INDEX default:default.idx1'             'grep "error\|cause\|success" -i'
run_query 'DROP INDEX default:`beer-sample`.idx2'       'grep "error\|cause\|success" -i'

# create
run_query 'CREATE INDEX idx1 ON default:default(age) USING GSI'       'grep "error\|cause\|success" -i'
run_query 'CREATE INDEX idx2 ON default:`beer-sample`(abv) USING GSI' 'grep "error\|cause\|success" -i'

# drop
run_query 'DROP INDEX default:default.idx1'             'grep "error\|cause\|success" -i'
run_query 'DROP INDEX default:`beer-sample`.idx2'       'grep "error\|cause\|success" -i'

# create
run_query 'CREATE INDEX idx1 ON default:default(age) USING GSI'       'grep "error\|cause\|success" -i'
run_query 'CREATE INDEX idx2 ON default:`beer-sample`(abv) USING GSI' 'grep "error\|cause\|success" -i'
