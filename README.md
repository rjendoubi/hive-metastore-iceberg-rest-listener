# Hive Metastore (HMS) Iceberg REST Listener

A [HMS MetaStoreEventListener](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r4.0.0/api/org/apache/hadoop/hive/metastore/MetaStoreEventListener.html)
which forwards JSON representations of Iceberg table events to a configured
REST endpoint. The subset of events sent are those required in order to
keep an up-to-date view of the table between an "owner" environment and a
"follower" environment. The follower environment must not alter the table
in any way; its access must be read-only.

# References

## https://github.com/mesmacosta/hive-custom-metastore-listener

Has an example of how to use [Jackson](https://github.com/FasterXML/jackson)'s
[ObjectMapper](https://www.baeldung.com/jackson-object-mapper-tutorial) to
dump Table objects out to logs.

Use this to serialize [Events](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r4.0.0/api/org/apache/hadoop/hive/metastore/events/ListenerEvent.html)?

## https://dzone.com/articles/create-your-own-metastoreevent-listeners-in-hive-w

Short and sweet example.

Uses a [MetaStorePreEventListener](https://svn.apache.org/repos/infra/websites/production/hive/content/javadocs/r4.0.0/api/org/apache/hadoop/hive/metastore/MetaStorePreEventListener.html),
but I think we want the regular MetaStoreEventListener.

## https://tejadogiparthi.medium.com/metadata-syncing-between-two-hive-metastores-d17a0c0a557

* Discussion of possible approaches
* Architecture diagram
* Listing of Event types
* Example JSON representation of an Event