# spark-on-hbase
* Spark optimised for scanning, joining and updating HBaseRdd
* The master setup is for HBase API 1.1.0.1, Scala 2.10 and Spark 1.4.1 but it is possible to create branches for older APIs

# features overview

* RegionPartitioner - precise if HBase table splits are managed manuaally or approximation to HBase partitioner if automatic region splitting used
* HKey - optimised multi-type salted key representation [hash][type][value] for managing even distribution of arbitrary key types and optimised fuzzy filtering
* HBaseRdd - extension of spark RDD that is capable of single-stage join using with pluggable implementations, provided are multiget lookup and filter scan joins
* HBaseTable - class for executing mutations of HBase from an RDD input - supports bulk operations as well as standard HBase Client operations
* HBaseJoin - using HBase ordered properties and multiget functionality, this abstract function is provided with several variants for optimised joins


# quick start on YARN

    - cp default-spark-env.template default-spark-env ...modify env.variable to match your environment
    - ./scripts/assemble.spark - this will clone spark into /usr/lib/spark, build assembly jar and put it into <hdfs path>
    - ./scripts/build.demo - this will build a spark-on-hbase-demo.jar which can be run as a shell or a job - see below



# configuration and scripts

...


