2i configuration parameters
===========================

**maxVbuckets** (int)
    number of vbuckets configured in KV

**endpoint.dataport.bufferSize** (int)
    number of entries to buffer before flushing it, where each entry is for a vbucket's set of mutations that was flushed by the endpoint.

**endpoint.dataport.bufferTimeout** (int)
    timeout in milliseconds, to flush vbucket-mutations from endpoint

**endpoint.dataport.harakiriTimeout** (int)
    timeout in milliseconds, after which endpoint will commit harakiri if not activity

**endpoint.dataport.keyChanSize** (int)
    channel size of dataport endpoints data input

**endpoint.dataport.maxPayload** (int)
    maximum payload length, in bytes, for transmission data from router to downstream client

**endpoint.dataport.remoteBlock** (bool)
    should dataport endpoint block when remote is slow ?

**indexer.adminPort** (string)
    port for index ddl and status operations

**indexer.clusterAddr** (string)
    Local cluster manager address

**indexer.enableManager** (bool)
    Enable index manager

**indexer.numVbuckets** (int)
    Number of vbuckets

**indexer.scanPort** (string)
    port for index scan operations

**indexer.scanTimeout** (int)
    timeout, in milliseconds, timeout for index scan processing

**indexer.storage_dir** (string)
    Index file storage directory

**indexer.streamCatchupPort** (string)
    port for catchup stream

**indexer.streamInitPort** (string)
    port for inital build stream

**indexer.streamMaintPort** (string)
    port for maintenance stream

**indexer.compaction.interval** (int)
    Compaction poll interval in seconds

**indexer.compaction.minFrag** (int)
    Compaction fragmentation threshold percentage

**indexer.compaction.minSize** (uint64)
    Compaction min file size

**log.level** (string)
    logging level for the system. allowable values are silent|fatal|error|warn|info|debug|trace.

**projector.clusterAddr** (string)
    KV cluster's address to be used by projector

**projector.colocate** (bool)
    Whether projector will be colocated with KV. In which case `kvaddrs` specified above will be discarded

**projector.feedChanSize** (int)
    channel size for feed's control path and back path.

**projector.feedWaitStreamEndTimeout** (int)
    timeout, in milliseconds, to await a response for StreamEnd

**projector.feedWaitStreamReqTimeout** (int)
    timeout, in milliseconds, to await a response for StreamRequest

**projector.kvAddrs** (string)
    Comma separated list of KV-address to read mutations, this need to exactly match with KV-node's configured address

**projector.mutationChanSize** (int)
    channel size of projector's data path routine

**projector.name** (string)
    human readable name for this projector

**projector.routerEndpointFactory** (common.RouterEndpointFactory)
    RouterEndpointFactory callback to generate endpoint instances to push data to downstream

**projector.vbucketSyncTimeout** (int)
    timeout, in milliseconds, for sending periodic Sync messages.

**projector.adminport.listenAddr** (string)
    projector's adminport address listen for request.

**projector.adminport.maxHeaderBytes** (int)
    in bytes, is max. length of adminport http header used by projector

**projector.adminport.name** (string)
    human readable name for this adminport, must be supplied

**projector.adminport.readTimeout** (int)
    timeout in milliseconds, is read timeout for adminport http server used by projector

**projector.adminport.urlPrefix** (string)
    url prefix (script-path) for adminport used by projector

**projector.adminport.writeTimeout** (int)
    timeout in milliseconds, is write timeout for adminport http server used by projector

**projector.client.exponentialBackoff** (int)
    multiplying factor on retryInterval for every attempt with server

**projector.client.maxRetries** (int)
    maximum number of timest to retry

**projector.client.retryInterval** (int)
    retryInterval, in milliseconds, when connection refused by server

**projector.client.urlPrefix** (string)
    url prefix (script-path) for adminport used by projector

**projector.dataport.indexer.genServerChanSize** (int)
    request channel size of indexer dataport's gen-server routine

**projector.dataport.indexer.maxPayload** (int)
    maximum payload length, in bytes, for receiving data from router

**projector.dataport.indexer.tcpReadDeadline** (int)
    timeout, in milliseconds, while reading from socket

**queryport.client.connPoolAvailWaitTimeout** (int)
    timeout, in milliseconds, to wait for an existing connection from the pool before considering the creation of a new one

**queryport.client.connPoolTimeout** (int)
    timeout, in milliseconds, is timeout for retrieving a connection from the pool

**queryport.client.maxPayload** (int)
    maximum payload, in bytes, for receiving data from server

**queryport.client.poolOverflow** (int)
    maximum number of connections in a pool

**queryport.client.poolSize** (int)
    number simultaneous active connections connections in a pool

**queryport.client.readDeadline** (int)
    timeout, in milliseconds, is timeout while reading from socket

**queryport.client.writeDeadline** (int)
    timeout, in milliseconds, is timeout while writing to socket

**queryport.indexer.maxPayload** (int)
    maximum payload, in bytes, for receiving data from client

**queryport.indexer.pageSize** (int)
    number of index-entries that shall be returned as single payload

**queryport.indexer.readDeadline** (int)
    timeout, in milliseconds, is timeout while reading from socket

**queryport.indexer.streamChanSize** (int)
    size of the buffered channels used to stream request and response.

**queryport.indexer.writeDeadline** (int)
    timeout, in milliseconds, is timeout while writing to socket
