Thrift Connector
================

Thrift Connector makes it possible to integrate with external storage systems without Presto code modification.

In order to do it an external system needs to provide Thrift Nodes implementing a given Thrift service interface.
Thrift Nodes are responsible for providing metadata and data in a form that Thrift Connector expects.
They are assumed to be stateless and independent from each other.

Using Thrift Connector over a custom Presto connector can be especially useful in the following cases.

* Java client for a storage system is not available.
By using Thrift as transport and service definition Thrift Connector can integrate with systems written in non-Java languages.

* Storage system's model doesn't easily map to metadata/table/row concept or there are multiple ways to do it.
For example, there are multiple ways how to map data from a key/value storage to relational representation.
Instead of supporting all of the variations in the connector this task can be moved to the external system itself.

* You cannot or don't want to modify Presto code base to add a custom connector to support your storage system.

See description of thrift interface that needs to be implemented in `presto-thrift` project.
