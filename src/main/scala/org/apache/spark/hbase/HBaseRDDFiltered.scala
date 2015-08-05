package org.apache.spark.hbase

import org.apache.hadoop.hbase.client.Result

/**
 * Created by mharis on 05/08/15.
 */
class HBaseRDDFiltered[K,V](self: HBaseRDD[K,V], filter: HBaseFilter)
  extends HBaseRDD[K,V](self.sc, self.tableNameAsString, self.filters :+ filter) {
  override def bytesToKey: (Array[Byte]) => K = self.bytesToKey

  override def keyToBytes: (K) => Array[Byte] = self.keyToBytes

  override def resultToValue: (Result) => V = self.resultToValue
}
