## Publisher subscriber

Publisher-subscriber is an event framework across the network. It is based on
`topics`, to which network subscriptions are allowed. The event framework is
actively managed by Index-Coordinator, and for every topic created or delete
and for every node subscribed or un-subscribed Index-Coordinator will manage
them as part of its StateContext.

#### Topic

Each topic is represented by following structure,

```go
    type Topic struct {
        name        string
        subscribers []string
    }
```

- `name` is represented in path format, and subscribe / un-subscribe APIs will
  accept glob-pattern on name.
- `subscribers` is a list of connection string, where each connection string
  is represented in **host:port** format.

Subscribers should ensure that there is a thread/routine listening at the
specified port. Topic publishers will get the latest list of subscribers on
its topic, request a TCP connection with the subscriber and start pushing one
or more events to it.  It is up to the publisher and the subscriber to agree
upon the connection, transport and protocol details.

**Note:** In future we might extend this definition to make it easy for
third-party components to interplay with secondary indexing system

### Special topic - TopicTopic

TopicTopic is a special kind of topic than can selectively publish subscription
changes on any other topic.

#### /topic/_path_

If a component want to be notified whenever a topic specified by `path`
changes, happens when a topic or a subscriber to topic is added or deleted,
process can subscribe to `/topic`, suffixed by the `path`.

**Publisher: Index-Coordinator**, when ever Index-Coordinator makes a change
to an active topic or to the list of Topics, it will check for a topic name
`/topic/<path>`, and push those changes to topictopic's subscribers.

event format,
```json
{
    "topic":       "/topic/<path>",
    "topic-name":  "<path>",
    "subscribers": <array-of-connection-string>
}
```

### Standard topics and publishers

Following are standard collection of topics and its publishers.

#### /indexinfos

Subscribers will be notified when any index DDL changes in the StateContext.

**Publisher: Index-Coordinator**, when ever Index-Coordinator makes a change
to an index or to the list of index-information, it will publish the following
event,

```json
{
    "topic":     "/indexinfos",
    "indexinfo": <property-of-index-information>,
}
```

#### /indexinfos/_indexid_

Subscribers will be notified when index DDL for _indexid_ changes in the
StateContext.

**Publisher: Index-Coordinator**, when ever Index-Coordinator makes a change
to index, `indexid`, it will publish the following event,

```json
{
    "topic":     "/indexinfos/<indexid>",
    "indexinfo": <property-of-index-information>,
}
```
