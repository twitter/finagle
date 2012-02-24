package com.twitter.finagle.memcached

import org.specs.Specification
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import com.twitter.conversions.time._

class MemcachedOutputFormatSpec extends Specification {
  "MemcachedOutputFormat" should {
    
    doBefore {
      FauxMemcachedClient.map.clear
    }

    "write to the memcached" in {
      val id = new TaskAttemptID() 
      val conf = new Configuration
      val client = new FauxMemcachedClientFactory()
      MemcachedOutputFormat.setFactory(conf, client)
      val context = new TaskAttemptContext(conf, id)
      val of = new MemcachedOutputFormat
      val writer = of.getRecordWriter(context)
      writer.write(new Text("hello"), new BytesWritable("world".getBytes))
      
      new String(FauxMemcachedClient.map.get("hello")) mustEqual "world"
    }
    
    
    "write to the memcached with delay" in {
      val id = new TaskAttemptID() 
      val conf = new Configuration
      val client = new FauxMemcachedClientFactory(100.millis)
      MemcachedOutputFormat.setFactory(conf, client)
      val context = new TaskAttemptContext(conf, id)
      val of = new MemcachedOutputFormat
      val writer = of.getRecordWriter(context)
      writer.write(new Text("hello"), new BytesWritable("world".getBytes))
      writer.close(context)
      new String(FauxMemcachedClient.map.get("hello")) mustEqual "world"
    }
  }
}
