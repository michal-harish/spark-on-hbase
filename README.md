# spark-on-hbase
* Spark optimised for scanning, joining and updating HBaseRdd
* The master setup is for HBase API 1.1.0.1, Scala 2.10 and Spark 1.4.1 but it is possible to create branches for older APIs

# features overview

* __HKeySpace__ and __HKey__ - abstract classes for defining different types of key, a small but integral piece of code. All the keys, whether HBase row key or Pair RDD keys are represented as HKey and each HKey is defined by HKeySpace type and byte array value. This ensures that each key stores it's salt and meta-data about the type in the first 6 bytes when serialised into a byte array.
* __RegionPartitioner__ - extension of spark Partitioner that uses HBase internals to create key ranges for given number of regions - it emulates precisely the behaviour of HBase partitioner if HBase table splits are managed manually or works as a good approximation to HBase partitioner if automatic region splitting used. Note that approximation still works good because each spark task talks only to one region if there's more partitions that regions and only to a few regions if there is more regions than spark partitions.
* __HBaseRdd__ - extension of spark RDD that is capable of single-stage join using with pluggable implementations, provided are multiget lookup and filter scan joins
* __HBaseTable__ - class for executing mutations of HBase from an RDD input - supports bulk operations as well as standard HBase Client operations
* __HBaseJoin__ - using HBase ordered properties and multiget functionality, this abstract function is provided with several variants for optimised joins


# quick start running DEMO on YARN cluster

First thing you'll need is a deafult-spark-env, there's a template you can copy and then modify to match your environment.

```cp default-spark-env.template default-spark-env```
    
If you don't have your spark assembly jar ready on the driver or available in hdfs for executors, you'll first need to build it and put it on the driver and into hdfs:

```./scripts/build spark```
    
You can then use build script to build a demo application, which will package and prepare start shell and submit scripts:

```./scripts/build demo```

You can then run the demo appliation as a shell:

```./scripts/demo-spark-shell```
