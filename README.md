# kv-utils

Abstracted helper classes providing consistent key-value store functionality, with zookeeper and etcd3 implementations.

- [`KVTable`](src/main/java/com/ibm/watson/kvutils/KVTable.java) - A consistent key-value cache which is automatically kept in sync with a centralized KV store such as etcd or zookeeper.
- [`DynamicConfig`](src/main/java/com/ibm/watson/kvutils/DynamicConfig.java) - Simple read-only string config map automatically kept in sync with a shared table of configuration which allows listeners to react to config changes.
- [`SharedCounter`](src/main/java/com/ibm/watson/kvutils/SharedCounter.java) - A basic distributed atomic counter.
- [`LeaderElection`](src/main/java/com/ibm/watson/kvutils/LeaderElection.java) - Leader election, with an observer mode and ability to listen for leadership changes.
- [`Distributor`](src/main/java/com/ibm/watson/kvutils/Distributor.java) - [Rendezvous hashing](https://en.wikipedia.org/wiki/Rendezvous_hashing) implementation for group of members, which implements `IntPredicate`.
- [`SessionNode`](src/main/java/com/ibm/watson/kvutils/SessionNode.java) - A singleton value whose existence is linked to the kv store client's session.

It is recommended to instantiate any/all of these classes via a [`KVUtilsFactory`](src/main/java/com/ibm/watson/kvutils/factory/KVUtilsFactory.java).


### History

Though not reflected in the commit history of this repository, this library was originally developed and used internally by IBM in early 2017, with most development done in 2017-2018. A decision was made to contribute it to open source in 2021.