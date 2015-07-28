This is a generic extension of spark for efficient scanning, joining and updating HBase tables from a spark environment. The master setup is for HBase API 1.1.0.1, Scala 2.10 and Spark 1.4.1 but it is possible to create branches for older APIs simply by changing the versions properties in the pom.xml (dataframes api is not necessary for the basic use case so practically any spark version > 0.92 should work but for HBase old API some refactoring will be required).

Its main concepts are __`HBaseRDD`__ and `HBaseTable`. HBaseRDD is used for scanning and optimised single-stage joins whil HBaseTable is for mutating underlying hbase table using RDDs as input.

It can be used in 3 major ways:
- Basic: In the most basic case it can be used to simply map existing hbase tables to HBaseRDD which will result a simple pair RDD[(Array[Byte], hbase.client.Result)]
- Standard: In the more typical case, by extending HBaseRDDBase and HBaseTable to provide mapping of raw key bytes and hbase result to some more meaningful types. You can learn about this method by studying the demo-simple application
- Advanced: Using the keyspace extension to the basic HBaseRDD and HBaseTable. This extension cannot be used on existing tables because it uses predefined key structure which aims to get the most from both spark and hbase perspective. You can learn more about this method by studing the demo-graph application.


# quick start

First thing you'll need is a deafult-spark-env, there's a template you can copy and then modify to match your environment.

```cp scripts/default-spark-env.template scripts/default-spark-env```

On the yarn nodes as well as driver, the following files should be distributed:
```/usr/lib/hbase/lib``` needs to contain all hbase java libraries required by the hbase client
```/usr/lib/hbase/lib/native``` needs to contain all required native libraries for compression algorithms etc.

Further, on the driver you'll need the distributions of spark and hadoop as defined in the pom.xml and on the path defined by `$SPARK_HOME/` and `$HADOOP_HOME` in the spark-default-env respectively
NOTE: that the scripts predefined here for spark-shell and spark-submit define the spark master as yarn-client so the driver is the computer from which you are building the demo app.

If you don't have your spark assembly jar ready on the driver or available in hdfs for executors, you'll first need to build it and put it on the driver and into hdfs:

```./scripts/build spark```

# quick start - 1 - mapping an existing table to an instance of HBaseRdd[(Array[Byte], hbase.client.Result)]

```...TODO```

# quick start - 2 - running a simple demo on YARN cluster

Run the following script which will package the demo application in org.apache.spark.hbase.examples.demo.simple
```./scripts/build demo-simple```

You can then run the demo appliation as a shell:

`./scripts/demo-simple-shell`

# quick start - 3 - running an advance graph demo on YARN cluster

Run the following script which will package the demo application in org.apache.spark.hbase.examples.demo.graph
```./scripts/build demo-graph```

You can then run the demo appliation as a shell:

`./scripts/demo-graph-shell`

# TODOs

- Unify key value mappers under a single interface
- Add implicit Ordering[K] for all HBaseRDD representations, as HBase is ordered by definition
- Refactor for enabling forks and make the graph demo an example of a fork
