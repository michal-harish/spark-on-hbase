package org.apache.spark.hbase

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.conf.Configuration

/**
 * Created by mharis on 21/07/15.
 */
trait HBaseDescriptors  {

  def getNumRegions(hbaseConf: Configuration, tableName: TableName) = {
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val regionLocator = connection.getRegionLocator(tableName)
    try {
      regionLocator.getStartKeys.length
    } finally {
      regionLocator.close
      connection.close
    }
  }
  def getRegionSplits(hbaseConf: Configuration, tableName: TableName) = {
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val regionLocator = connection.getRegionLocator(tableName)
    val keyRanges = regionLocator.getStartEndKeys
    regionLocator.close
    connection.close
    keyRanges.getFirst.zipWithIndex.map { case (startKey, index) => {
      (startKey, keyRanges.getSecond()(index))
    }}
  }
}
