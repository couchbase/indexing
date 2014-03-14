## Publisher subscriber

Publisher subscriber is an event framework across the network. It is based on
`topics`, to which network subscriptions are allowed. The event framework is
actively managed by Index-Coordinator, and for every new topic created and
when new nodes are subcribing to the topics, Index-Coordinator shall replicat
the changes to topic list.
