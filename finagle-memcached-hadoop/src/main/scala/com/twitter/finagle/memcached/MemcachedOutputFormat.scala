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
import com.twitter.util._
import com.twitter.conversions.time._
import org.apache.commons.codec.binary.Base64
import _root_.java.io._
import _root_.java.util.Date
import _root_.java.util.concurrent.atomic._

object MemcachedOutputFormat {
  val CLIENT_FACTORY = "memcached_client_factory"
  val MAX_CONCURRENCY = "memcached_max_concurrency"
  val TRIES_LIMIT = 3
  val MIN_SLEEP = 1.second
  val PROGRESS_EVERY = 10000

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
    println("Opening MemcachedOutputFormat#RecordWriter")
    val client = memcachedClientFactory(taskContext).newInstance()
    val timer = new JavaTimer()
    var written = new AtomicLong()
    var pending = new AtomicLong()
    var failures = new AtomicLong()
    var retries = new AtomicLong()
    var ops = new AtomicLong()

    def write(key: Text, value: BytesWritable): Unit = {
      Thread.sleep(pending.get() / 100)
      if(ops.get() % PROGRESS_EVERY == 0) {
        val now = new Date()
        println("MemcachedOutputFormat#RecordWriter status ["+now+
            "] (started: "+ops.get()+
            ", written: "+written.get()+
            ", pending: "+pending.get()+
            ", failures: "+failures.get()+
            ", retries: "+retries.get()+")")
        taskContext.progress()
      }
      write(key, value, 0)
      ops.incrementAndGet()
    }

    def write(key: Text, value: BytesWritable, tries: Int): Unit = {
      if (tries == TRIES_LIMIT) {
        failures.incrementAndGet()
        return
      }

      pending.incrementAndGet()
      client.put(key.toString, value.getBytes).map { x =>
        pending.decrementAndGet()
        written.incrementAndGet()
      } onFailure { throwable =>
        retries.incrementAndGet()
        pending.decrementAndGet()
        timer.doLater(MIN_SLEEP * (1 << tries))(write(key, value, tries + 1))        
      }
    }

    def close(taskContext: TaskAttemptContext) = {
      while(pending.get() > 0) {
        println("Closing MemcachedOutputFormat#RecordWriter, waiting on "+pending.get()+" records to finish.")
        Thread.sleep(MIN_SLEEP.inMillis)
      }
      timer.stop()
      client.release()
      println("Closed MemcachedOutputFormat#RecordWriter.")
    }
  }
}