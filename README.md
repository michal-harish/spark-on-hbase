# spark-on-hbase
Spark optimised for scanning, joining and updating HBaseRdd

# features overview

* RegionPartitioner - precise if HBase table splits are managed manuaally or approximation to HBase partitioner if automatic region splitting used
* HKey - optimised multi-type salted key representation [hash][type][value] for managing even distribution of arbitrary key types and optimised fuzzy filtering
* HBaseRdd - custom implementation of spark RDD that is capable of single-stage join using with pluggable implementations, provided are multiget lookup and filter scan joins


# quick start on YARN

...

# configuration and scripts

...

