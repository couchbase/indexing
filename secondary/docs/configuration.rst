2i configuration parameters
===========================

**maxVbuckets** (int)
    number of vbuckets configured in KV

**log.ignore** (bool)
    ignores all logging, irrespective of the log-level

**log.level** (string)
    logging level for the system

**projector.clusterAddr** (string)
    KV cluster's address to be used by projector

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
    timeout, in milliseconds, is for sending periodic Sync messages for

**projector.adminport.listenAddr** (string)
    http bind address for this projector's adminport

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

**projector.dataport.client.bufferTimeout** (int)
    timeout in milliseconds, to flush endpoint buffer to remote

**projector.dataport.client.genServerChanSize** (int)
    request channel size of projector-dataport-client's gen-server routine

**projector.dataport.client.harakiriTimeout** (int)
    timeout in milliseconds, after which endpoint will commit harakiri if not activity

**projector.dataport.client.keyChanSize** (int)
    channel size of dataport endpoints data input

**projector.dataport.client.maxPayload** (int)
    maximum payload length, in bytes, for transmission data from router to downstream client

**projector.dataport.client.mutationChanSize** (int)
    channel size of projector-dataport-client's data path routine

**projector.dataport.client.noRemoteBlock** (bool)
    should dataport endpoint block when remote is slow ?

**projector.dataport.client.parConnections** (int)
    number of parallel connections to open with remote

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

**queryport.indexer.writeDeadline** (int)
    timeout, in milliseconds, is timeout while writing to socket
