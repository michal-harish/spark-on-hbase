package org.apache.spark.hbase.demo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.hbase._
import org.apache.spark.hbase.keyspace._

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[HKeySpace])
    kryo.register(classOf[HKeySpaceUUID])
    kryo.register(classOf[HKeySpaceUUIDNumeric])
    kryo.register(classOf[HKeySpaceLong])
    kryo.register(classOf[HKeySpaceString])
    kryo.register(classOf[HKey])
    kryo.register(classOf[HE])
  }
}