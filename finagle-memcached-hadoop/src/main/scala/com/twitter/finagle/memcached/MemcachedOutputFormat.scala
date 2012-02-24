package com.twitter.finagle.memcached

import org.apache.hadoop.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.JobContext
import  _root_.java.util.concurrent.Semaphore
import org.apache.hadoop.mapreduce.OutputCommitter
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import MemcachedOutputFormat._
import org.apache.commons.codec.binary.Base64
import _root_.java.io._

object MemcachedOutputFormat {
  val CLIENT_FACTORY = "memcached_client_factory"
  val MAX_CONCURRENCY = "memcached_max_concurrency"
  val MAX_CONCURRENCY_DEFAULT = 100
  val TRIES_LIMIT = 3
  val MIN_SLEEP = 1000
  
  def setFactory(config: Configuration, factory: SerializableKeyValueClientFactory) = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(factory)
    oos.close
    config.set(CLIENT_FACTORY, Base64.encodeBase64String(baos.toByteArray))
  }
}

class MemcachedOutputFormat extends OutputFormat[Text, BytesWritable] {
    
  def checkOutputSpecs(jobContext: JobContext) = {
    val conf = jobContext.getConfiguration
    if(conf.getBoolean("mapred.reduce.tasks.speculative.execution", true)) {
      throw new RuntimeException("Speculative execution should be false");
    }
  }
    
  def getOutputCommitter(context: TaskAttemptContext) = new OutputCommitter {
    def abortTask(taskContext: TaskAttemptContext) = {}
    def cleanupJob(jobContext: JobContext) = {}
    def commitTask(taskContext: TaskAttemptContext) = {}
    def needsTaskCommit(taskContext: TaskAttemptContext) = false
    def setupJob(jobContext: JobContext) = {}
    def setupTask(taskContext: TaskAttemptContext) = {}
  }
  
  private[memcached] def memcachedClientFactory(taskContext: TaskAttemptContext) = {
    val string = taskContext.getConfiguration.get(CLIENT_FACTORY)
    val bytes  = Base64.decodeBase64(string)
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    ois.readObject().asInstanceOf[SerializableKeyValueClientFactory]
  }
  
  def getRecordWriter(taskContext: TaskAttemptContext) = new RecordWriter[Text, BytesWritable] {
    val concurrencyString = taskContext.getConfiguration.get(MAX_CONCURRENCY)
    val concurrency = if(concurrencyString != null) {
      concurrencyString.toInt
    } else {
      MAX_CONCURRENCY_DEFAULT
    }
    val semaphore = new Semaphore(concurrency)
    val client = memcachedClientFactory(taskContext).newInstance()
    var written = 0
    
    def write(key: Text, value: BytesWritable): Unit = write(key, value, 0)
    
    def write(key: Text, value: BytesWritable, tries: Int): Unit = {
      semaphore.acquire
      client.put(key.toString, value.getBytes).map { x => 
        semaphore.release
        written += 1
      } onFailure { throwable =>
        Thread.sleep(MIN_SLEEP * (1 << tries))
        semaphore.release
        write(key, value, tries + 1)
      }
    }
    
    def close(taskContext: TaskAttemptContext) = {
      semaphore.acquire(concurrency)
      client.release()
    }
  }
}