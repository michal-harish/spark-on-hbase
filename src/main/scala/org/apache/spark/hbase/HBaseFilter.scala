package org.apache.spark.hbase

/**
 * Created by mharis on 05/08/15.
 */
trait HBaseFilter extends Serializable {

  def configureQuery(query: HBaseQuery): Unit;

}


