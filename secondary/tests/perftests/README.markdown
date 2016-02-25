## Perf Tests for GSI CI 

# Status
   Authored basic tests and running them. Will add more tests after inital set of tests run consistently

# Usage
	Pre-reqs:
	1. MUST set below url
	export CBAUTH_REVRPC_URL="http://<username>:<password>@<ip:port>/query"
	Example:
	export CBAUTH_REVRPC_URL="http://Administrator:asdasd@127.0.0.1:9000/query"
	
	2. Build cbindexperf and n1qlperf tools
	cd cmd/cbindexperf
	go build
	cd tools/n1qlperf
	go build
	
	ToDo:  Make this part of code. Currently there is an error even after setting env in code
	
	StorageMode:
    To run in memdb mode, specify  "IndexUsing": "memdb" in cbconfig file
	To run in forestdb mode, specify "IndexUsing": "gsi" in cbconfig file
	
	Scan tool to use:
	The scan perf tests can be run cbindexperf or n1qlperf tool
	Use -perftool switch to specify cbindexperf or n1qlperf as tool to be used for scanning
	
	Example:
	go test -v -test.run TestPerfScanLatency_Lookup_StaleOk  -perftool cbindexperf
	go test -v -test.run TestPerfScanLatency_Lookup_StaleOk  -perftool n1qlperf
	
	Number of documents to load:
	Use -numdocs switch to specify number of documents to load
	
	Marking test pass/fail: NA for now
	TODO - the below section is not finalized. For now, there is no pass/fail support
	The same tests can be run in various environments. To determine pass/fail, pass minimum throughput, average throughput and maximum build time as parameters to the go test command
	
	Example:
	go test -v -test.run TestPerfInitialIndexBuild -maxbuildtime 60  
	go test -v -test.run TestPerfScanLatency_Lookup_StaleOk  -perftool cbindexperf -minthroughput 19000 -maxlatency 10000000
	go test -v -test.run TestPerfScanLatency_Lookup_StaleOk  -perftool n1qlperf -minthroughput 19000 -maxlatency 12000000
	
	The throughput and latency numbers also change between n1qlperf tool and cbindexperf tool and hence needs to be provided depending on the tool used
