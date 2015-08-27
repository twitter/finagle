package com.twitter.finagle.memcached

import com.twitter.io.Buf
import com.twitter.util._
import _root_.java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import _root_.java.util.Random

/**
 * This class is designed to support replicated memcached setups.  It supports a limited
 * subset of operations (just get, set, and delete).
 */
class PoolingReadRepairClient(allClients: Seq[BaseClient[Buf]],
                              readRepairProbability: Float,
                              readRepairCount: Int = 1,
                              futurePool: FuturePool = new ExecutorServiceFuturePool(Executors.newCachedThreadPool())) extends Client {

  val rand = new Random()

  def getResult(keys: Iterable[String]) = {
    val clients = getSubsetOfClients()
    val futures = clients.map(_.getResult(keys))
    if (futures.size == 1) {
      // Don't bother being fancy, just GTFO
      futures.head
    } else {
      // We construct a return value future that we will update manually ourselves
      // to accomodate the more complicated logic.
      val answer = new Promise[GetResult]

      // First pass: return the first complete, correct answer from the clients
      val futureAttempts = futures.map { future =>
        future.map { result =>
          if(result.hits.size == keys.size) {
            answer.updateIfEmpty(Try(result))
          }
          result
        }
      }

      // Second pass
      Future.collect(futureAttempts).map { results =>
        // Generate the union of the clients answers, and call it truth
        val canon = results.foldLeft(new GetResult())(_++_)

        // Return the truth, if we didn't earlier
        answer.updateIfEmpty(Try(canon))

        // Read-repair clients that had partial values
        results.zip(clients).map { tuple =>
          val missing = canon.hits -- tuple._1.hits.keys
          missing.map { hit =>
            set(hit._1, hit._2.value)
          }
        }
      }

      answer
    }
  }

  def getSubsetOfClients() = {
    val num = if (rand.nextFloat < readRepairProbability) readRepairCount + 1 else 1
    val buf = new ArrayBuffer[BaseClient[Buf]]
    allClients.copyToBuffer(buf)
    while(buf.size > num) {
      buf.remove(rand.nextInt(buf.size))
    }
    buf.toSeq
  }

  def release() = allClients.map(_.release())
  def set(key: String, flags: Int, expiry: Time, value: Buf) = {
    val futures = allClients.map(_.set(key, flags, expiry, value))
    val base = futures.head
    futures.tail.foldLeft(base)(_.or(_))
  }

  def delete(key: String) = Future.collect(allClients.map(_.delete(key))).map(_.exists(x=>x))

  def getsResult(keys: Iterable[String]) = unsupported
  def stats(args: Option[String]) = unsupported
  def decr(key: String,delta: Long) = unsupported
  def incr(key: String,delta: Long) = unsupported
  def cas(key: String, flags: Int,expiry: Time, value: Buf,casUnique: Buf) = unsupported
  def replace(key: String,flags: Int,expiry: Time,value: Buf) = unsupported
  def prepend(key: String,flags: Int,expiry: Time,value: Buf) = unsupported
  def append(key: String,flags: Int,expiry: Time,value: Buf) = unsupported
  def add(key: String,flags: Int,expiry: Time,value: Buf) = unsupported
  private def unsupported = throw new UnsupportedOperationException
}
