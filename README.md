This is a generic extension of spark for efficient scanning, joining and mutating HBase tables from a spark environment. The master setup is for HBase API 1.1.0.1, Scala 2.10 and Spark 1.4.1 but it is possible to create branches for older APIs simply by changing the versions properties in the pom.xml (dataframes api is not necessary for the basic use case so practically any spark version > 0.92 should work but for HBase old API a small refactor  will be required around the hbase api calls).

This library can be used in 3 major ways:
- __Basic__: In the most basic case it can be used to simply map existing hbase tables to HBaseRDD which will result a simple pair RDD[(Array[Byte], hbase.client.Result)]. These can be filtered, transformed,.. as any other RDD and the result can be for example given to HBaseTable as a mutation to execute on the same underlying table.
- __Standard__: In the more typical case, by extending HBaseTable to provide mapping of raw key bytes and hbase result to some more meaningful types. You can learn about this method in the Concepts section below and by studying the example source of the demo-simple application
- __Specialised/Experimental__: Using the keyspace extension to the basic HBaseRDD and HBaseTable. This extension cannot be used on existing tables because it uses predefined key structure which aims to get the most from both spark and hbase perspective. You can learn more about this method by studying the demo-graph application (the demo is broken in the TODOs below)

# Concepts

The main concept is __`HBaseTable`__ which behaves similarly to the Spark DataFrame API but the push down logic is handled slightly differently.
Any instance of `HBaseTable` is mapped to the underlying hbase table and its method `.rdd()` gives an instance of __`HBaseRDD`__
which inherits all transformation methods from RDD[(K,V)] and has some special transformations available via implicit conversions:
- __`myTable.rdd.filter(Consistency)`__ - server-side scan filter for different levels of consistency required
- __`myTable.rdd.filter(minStamp, maxStamp)`__ - server-side scan filter for hbase timestamp ranges
- __`myTable.rdd.select(columnOrFamily1, columnOrFamily2, ...)`__ - server-side scan filter for selected columns or column families
- __`myTable.rdd.join(other: RDD)`__ - uses a `HBaseJoin` abstract function which is implemented in 2 versions, both resulting in a single-stage join regardless of partitioners used. One is for situations where the right table is very large portion of the left hbase table - `HBaseJoinRangeScan` and the other is for situtations where the right table is a small fraction of the left table - `HBaseJoinMultiGet`. (The mechanism for choosing between the types of join is not done, i.e. at the moment all the joins are mutli-get, see TODO below)
- __`myTable.rdd.rightOuterJoin(other: RDD)`__ - uses the same optimized implementation as join but with rightOuterJoin result
- __`myTable.rdd.fill(range: RDD)`__ - Fill is an additional functionality, similar to join except where the argument rdd is treated as to be 'updated' or 'filled-in' where the value of the Option is None - this is for highly iterative algorithms which start from a subset of HBase table and expand it in later iterations.

Because these methods are available implicitly for any HBaseRDD or its extension they can be wrapped in additional layers
via HBaseRDDFiltered that are put together only when a compute method is invoked by a Spark action adding filters,
ranges etc to the single scan for each region/partition.

Besides HBaseTable and HBaseRDD another important concept is __`Transformation`__ which is a bi-directional mapper
that can map a basic hbase result value into a type V and inversely, a type V into a Mutation and which also declares
columns or column families which are required by it. This gives the HBaseTable extensions a very rich high-level
interface and at the same time optimizes scans which can be filtered under the hood to only read data necessary
for a given transformation. Transformations can be used with the following HBaseTable methods

- __`myTable.select(Transformation1, Transformation2, ...)`__ - selects only fields required by the selected Transformations and returns HBaseRDD[K, (T1, T2,...)]
- __`myTable.update(Transformation1, RDD[(K, T1)])`__ - transforms the input RDD, generate region-aware mutations and executes the multi-put mutation on the underlying table
- __`myTable.bulkUpdate(Transformation1, RDD[(K, T1)])`__ - same as update but not using hbase client API but generating HFiles and submitting them to the HBase Master (see notes below about the bulk operations)


## Bulk operations
__`bulkUpdate`__, __`bulkLoad`__ and __`bulkDelete`__ are available to generate HFiles directly for large mutations.
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

# example 3 - comprehensive tutorial with large scale operations

Consider a following example of a document table in which each row represents a document,
keyed by UUID and which has one column family 'A' where each column represents and Attribute of the document
and column family 'C' which always has one column 'body' containing the content of the document:

```
val documents = new HBaseTable[UUID](sc, "my_documents_table") {
    override def keyToBytes = (key: UUID) => ByteUtils.UUIDToBytes(key)
    override def bytesToKey = (bytes: Array[Byte]) => ByteUtils.bytesToUUID(bytes)
    
    val C = Bytes.toBytes("C")
    val body = Bytes.toBytes("body")
    val A = Bytes.toBytes("A")
    val spelling = Bytes.toBytes("spelling")

    val Content = new Transformation[String]("C:body") {
    
        override def apply(result: Result): String = {
          val cell = result.getColumnLatestCell(C, body)
          Bytes.toString(cell.getValueArray, cell.getValueOffset)
        }

        override def applyInverse(value: String, mutation: Put) {
          mutation.addColumn(C, body, Bytes.toBytes(value))
        }
    }
    
    val Spelling = new Transformation[String]("A:spelling") {
    
        override def apply(result: Result): String = {
          val cell = result.getColumnLatestCell(A, spelling)
          Bytes.toString(cell.getValueArray, cell.getValueOffset)
        }

        override def applyInverse(value: String, mutation: Put) {
          mutation.addColumn(A, spelling, Bytes.toBytes(value))
        }
    }
}
```

Above we have created (and implemented) a fully working HBaseTable instance with 2 trasnformations available that
can be used to read and write the data as typed RDD. We can for exmple do the following:

```
val dictionary: RDD[String] ... //contains a dictionary of known enlgish words

val docWords: RDD[(UUID, Seq[String])] = documents.select(documents.Content).mapValues(_.split("\\s+"))

val words = content.flatMap{ case (uuid, words) => words.map(word => (word, uuid))}

val misspelled = words.subtract(dictionary).map{ case (word, uuid) => (uuid, word) }.groupByKey

documents.update(documents.Spelling, misspelled.mapValues(words => "spelling" -> words.mkString(",")))
```

If we wanted to access the whole column family 'A' as a Map[String,String] of Attribute-Value pairs, we could 
add another transformation to the table:

```
    ...
    val Attributes = new Transformation[Map[String,String]]("A") {

        override def apply(result: Result): Map[String, Double] = {
          val builder = Map.newBuilder[String, String]
          val scanner = result.cellScanner
          while (scanner.advance) {
            val kv = scanner.current
            if (CellUtil.matchingFamily(kv, A)) {
              val attr = Bytes.toString(kv.getQualifierArray, kv.getQualifierOffset, kv.getQualifierLength)
              val value = Bytes.toString(kv.getValueArray, kv.getValueOffset)
              builder += ((attr, value))
            }
          }
          builder.result
        }

        override def applyInverse(value: Map[String, String], mutation: Put) {
          value.foreach { case (attr, value) => {
            mutation.addColumn(F, Bytes.toBytes(attr), Bytes.toBytes(value))
          }}
        }
    }
    ...
```

TODO section about bulk-loading the documents from some hdfs data and large-scale join and transformation

# example 4 - experimental stuff

This example makes use of the org.apache.spark.hbase.keyspace which provides specialised __`Key`__ implementation that
addresses several scalability issues and general integration between spark and hbase. It also provides specialised
__`HBaseRDDKS`__ and __`HBaseTableKS`__ which all share the concept of KeySpace. The main idea here is to be able
 to mix different key types while preserving even distribution of records across hbase regions. By using the form
 
 ``` [4-byte-hash] [2-byte-keyspace-symbol] [n-byte-key-value] ``` 
 
..it is also possible to do the hbase server-side fuzzy filtering for a specific 2-byte symbol by ignoring the first 4 bytes and matching 5th and 6th. Each implementation of KeySpace must provide serde methods for generating hash and value into pre-allocated byte array by the KeySpace abstraction. More information is in the comments of the demo application and the package classes.

Run the following script which will package the demo application in org.apache.spark.hbase.examples.demo.graph
```./scripts/build demo-graph```

You can then run the demo appliation as a shell:

`./scripts/demo-graph-shell`

# TODOs
- fix spark-submit script and add an example into the simple demo
- figure out a work-around for the bulk operations requiring the process to run under hbase user
- Mechanism for choosing HBaseJoin implementation must be done per operation, not simply configuration variable because the type of join depends on the relative volume of the RDDs not location or resource - but ideally it should be done by estimation, not requiring any control argument
- Write more comprehensive tutorial, generating larger table, writing a distributed algorithm over one column family and bulk-loading the result mutation into the second column family.
- Add implicit Ordering[K] for all HBaseRDD representations since HBase is ordered by definition
- investigate table.rdd.mapValues(Tags).collect.foreach(println) => WARN ClosureCleaner: Expected a closure; got org.apache.spark.hbase.examples.simple.HBaseTableSimple$$anon$2, while table.select(Tags) works fine


