This is a generic extension of spark for efficient scanning, joining and mutating HBase tables from a spark environment. The master setup is for HBase API 1.1.0.1, Scala 2.10 and Spark 1.4.1 but it is possible to create branches for older APIs simply by changing the versions properties in the pom.xml (dataframes api is not necessary for the basic use case so practically any spark version > 0.92 should work but for HBase old API a small refactor  will be required around the hbase api calls).

Its main concepts are __`HBaseRDD`__ and __`HBaseTable`__. `HBaseRDD` is used for scanning and optimised single-stage joins while `HBaseTable` is for mutating underlying hbase table using RDDs as input.

It can be used in 3 major ways:
- __Basic__: In the most basic case it can be used to simply map existing hbase tables to HBaseRDD which will result a simple pair RDD[(Array[Byte], hbase.client.Result)]. These can be filtered, transformed,.. as any other RDD and the result can be for example given to HBaseTable as a mutation to execute on the same underlying table.
- __Standard__: In the more typical case, by extending HBaseRDD and HBaseTable to provide mapping of raw key bytes and hbase result to some more meaningful types. You can learn about this method by studying the demo-simple application
- __Specialised/Experimental__: Using the keyspace extension to the basic HBaseRDD and HBaseTable. This extension cannot be used on existing tables because it uses predefined key structure which aims to get the most from both spark and hbase perspective. You can learn more about this method by studying the demo-graph application (the demo is broken in the TODOs below)

There is a couple of implcit conversion functions for HBaseRDD in __`HBaseRDDFunctions`__ which provide `.join` and `.lookup` alternatives. The `.join` uses a __`HBaseJoin`__ abstract function which is implemented in 2 versions, both resulting in a single-stage join regardless of partitioners used. One is for situations where the right table is very large portion of the left hbase table - __`HBaseJoinRangeScan` and the other is for situtations where the right table is a small fraction of the left table - __`HBaseJoinMultiGet`. (The mechanism for choosing between the types of join is not done, i.e. at the moment all the joins are mutli-get, see TODO below) Lookup is an additional functionality, similar to join except where the argument rdd is treated as to be 'updated' or 'looked-up' where the value of the Option is None - this is for highly iterative algorithms which use HBase as state.

__`bulkLoad`__ and __`bulkDelete`__ are available to generate HFiles directly for large mutations.
__NOTE:__ Due to the way how HBase handles the bulk files submission, the spark shell or job __needs to be started as `hbase` user__ in order to be able to use bulk operations.

# quick start (on YARN)

First thing you'll need is a deafult-spark-env, there's a template you can copy and then modify to match your environment.

```cp scripts/default-spark-env.template scripts/default-spark-env```

On the yarn nodes as well as driver, the following files should be distributed:
```/usr/lib/hbase/lib``` needs to contain all hbase java libraries required by the hbase client
```/usr/lib/hbase/lib/native``` needs to contain all required native libraries for compression algorithms etc.

Further, on the driver you'll need the distributions of spark and hadoop as defined in the pom.xml and on the path defined by `$SPARK_HOME/` and `$HADOOP_HOME` in the spark-default-env respectively
NOTE: that the scripts predefined here for spark-shell and spark-submit define the spark master as yarn-client so the driver is the computer from which you are building the demo app.

If you don't have your spark assembly jar ready on the driver or available in hdfs for executors, you'll first need to build it and put it on the driver and into hdfs.

# example 1 - basic use case

Mapping an existing table to an instance of HBaseRdd[(Array[Byte], hbase.client.Result)]

```
val sc: SparkContext = ...

val minStamp = HConstants.OLDEST_TIMESTAMP
val maxStamp = HConstants.LATEST_TIMESTAMP
val rdd1 = HBaseRDD.create(sc, "my-hbase-table", minStamp, maxStamp, "CF1:col1int", "CF1:col2double")

val cf1 = Bytes.toBytes("CF1")
val qual1 = Bytes.toBytes("col1int")
val qual2 = Bytes.toBytes("col2double")

val rdd2: RDD[String, (Int, Double)] = rdd1.map { case (rowKey, cells) => {
    val keyAsString = Bytes.toString(rowKey)
    val cell1 = cells.getColumnLatestCell(cf1, qual1)
    val cell2 = cells.getColumnLatestCell(cf1, qual2)
    val value1 = Bytes.toInt(cell1.getValueArray, cell1.getValueOffset)
    val value2 = Bytes.toDouble(cell2.getValueArray, cell2.getValueOffset)
    (keyAsString, (value1, value2))
}}
```

# example 2 - standard use case

Extending HBaseRDD class to provide richer semantics. The example is implemented as a demo application.

Run the following script which will package the demo application in org.apache.spark.hbase.examples.demo.simple

``` ./scripts/build demo-simple ```

You can then run the demo appliation as a shell:

``` ./scripts/demo-simple-shell ```

# example 3 - large scale operations
  TODO demo and section about joining and transforming large tables and using bulk operations

# example 4 - experimental stuff

This example makes use of the org.apache.spark.hbase.keyspace which provides specialised __`HKey`__ implementation that
addresses several scalability issues and general integration between spark and hbase. It also provides specialised
__`HBaseRDDHKey`__ and __`HBaseRDDTableHKey`__ which all share the concept of HKey. The main idea here is to be able
 to mix different key types while preserving even distribution of records across hbase regions. By using the form
 
 ``` [4-byte-hash] [2-byte-keyspace-symbol] [n-byte-key-value] ``` 
 
..it is also possible to do the hbase server-side fuzzy filtering for a specific 2-byte symbol by ignoring the first 4 bytes and matching 5th and 6th. Each implementation of HKeySpace must provide serde methods for generating hash and value into pre-allocated byte array by the HKeySpace abstraction. More information is in the comments of the demo application and the package classes.

Run the following script which will package the demo application in org.apache.spark.hbase.examples.demo.graph
```./scripts/build demo-graph```

You can then run the demo appliation as a shell:

`./scripts/demo-graph-shell`

# TODOs
- figure out a work-around for the bulk operations requiring the process to run under hbase user
- temporarily added serialization to HBaseTable to have it working, but only HBaseRDD should assumed to be serialized, need refactoring the mapper interface
- Mechanism for choosing HBaseJoin implementation must be done per operation, not simply configuration variable because the type of join depends on the relative volume of the RDDs not location or resource - but ideally it should be done by estimation, not requiring any control argument
- Write more comprehensive tutorial, generating larger table, writing a distributed algorithm over one column family and bulk-loading the result mutation into the second column family.
- Add implicit Ordering[K] for all HBaseRDD representations since HBase is ordered by definition
- Refactor for enabling forks and finish off the graph demo which currently doesn't really work
