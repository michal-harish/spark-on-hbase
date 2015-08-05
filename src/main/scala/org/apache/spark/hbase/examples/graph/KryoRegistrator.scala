package org.apache.spark.hbase.examples.graph

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.hbase.keyspace._

class KryoRegistrator extends org.apache.spark.serializer.KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[KeySpace])
    kryo.register(classOf[KeySpaceUUID])
    kryo.register(classOf[KeySpaceUUIDNumeric])
    kryo.register(classOf[KeySpaceLong])
    kryo.register(classOf[KeySpaceString])
    kryo.register(classOf[Key])
    kryo.register(classOf[HE])
  }
}