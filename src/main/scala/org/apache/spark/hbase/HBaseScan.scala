package org.apache.spark.hbase

import org.apache.hadoop.hbase.client.Scan

/**
 * Created by mharis on 05/08/15.
 */
trait HBaseScan {

  protected def configureRegionScan(scan: Scan) = {
    //RDDs derived from HBaseRDD can use this to add server-side filters etc.
  }

}
