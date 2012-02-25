package com.twitter.finagle.memcached

import org.apache.pig.StoreFunc
import org.apache.pig.data.Tuple
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.Job
import org.apache.pig.ResourceSchema
import org.apache.pig.data.DataType
import org.apache.hadoop.io._

class MemcachedPigStorage extends StoreFunc {
  def getOutputFormat = new MemcachedOutputFormat()

  var writer: RecordWriter[Text, BytesWritable] = null

  override def checkSchema(s: ResourceSchema) = {
    val fields = s.getFields
    if(fields.size != 2) {
      throw new RuntimeException("MemcachedPigStorage expects two fields")
    }
    if(fields(0).getType != DataType.CHARARRAY ||
       fields(1).getType != DataType.BYTEARRAY) {
      throw new RuntimeException("MemcachedPigStorage expects a chararray and a bytearray")
    }
  }

  def putNext(tuple: Tuple) = {
    val key = DataType.toString(tuple.get(0))
    val value = DataType.toBytes(tuple.get(1))
    writer.write(new Text(key), new BytesWritable(value))
  }

  def prepareToWrite(_writer: RecordWriter[_,_]) = {
    writer = _writer.asInstanceOf[RecordWriter[Text, BytesWritable]]
  }
  def setStoreLocation(s: String, j: Job) = {}
}
