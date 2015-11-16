package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.{CBToString, StringToChannelBuffer}
import com.twitter.util.Await
import java.util.UUID
import org.jboss.netty.buffer.ChannelBuffer
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.junit.JUnitRunner
import scala.collection.mutable

@Ignore
@RunWith(classOf[JUnitRunner])
final class BtreeClientIntegrationSuite extends FunSuite with BeforeAndAfterAll {
  var client: Client = _
  var dict: mutable.HashMap[String, mutable.HashMap[String, String]] = _

  override def beforeAll(): Unit = {
    // Until the Redis Server Btree changes are checked in and merged into master
    // The redis server needs to be started and shutdown externally
    // And the client connects it via the port 6379
    // After the changes become a part of the installed redis server
    // This will use RedisCluster to start and manage the external redis server
    val hostAddress = "127.0.0.1:6379"
    client = Client(hostAddress)
    dict = generateTestCases()
    assert(client != null)
  }

  override def afterAll(): Unit = {
    println("Closing client...")
    client.flushDB()
    client.release()
    println("Done!")
  }

  test("Correctly add outerkey, innerkey and value tuples using BADD command") {
    testBadd(client, dict)
  }

  test("Correctly return cardinality for outerkey using BCARD command") {
    testBcard(client, dict)
  }

  test("Correctly return value for outerkey, innerkey pair using BGET command") {
    testBget(client, dict)
  }

  test("Correctly return BRANGE from start to end for outerkey") {
    testBrange(client, dict)
  }

  test("Correctly return BRANGE from a start key that exists to the end for outerkey") {
    testBrangeInclusiveStart(client, dict)
  }

  test("Correctly return BRANGE from start to end key that exists for outerkey") {
    testBrangeInclusiveEnd(client, dict)
  }

  test("Correctly return BRANGE from start key to end key where both exist for outerkey") {
    testBrangeInclusiveStartEnd(client, dict)
  }

  test("Correctly return BRANGE from start key that doesn't exist to end for outerkey") {
    testBrangeExclusiveStart(client, dict)
  }

  test("Correctly return BRANGE from start to end key that doesn't exist for outerkey") {
    testBrangeExclusiveEnd(client, dict)
  }

  test("Correctly return BRANGE from start key to end key where both don't exist for outerkey") {
    testBrangeExclusiveStartEnd(client, dict)
  }

  test("Correctly return removal of innerkey value pairs for outerkey using BREM command") {
    testBrem(client, dict)
  }

  test("Correctly return cardinality function for outerkey using BCARD command") {
    testBcard(client, dict)
  }

  def defaultTest(client: Client) {
    val key = "megatron"
    val value = "optimus"

    println("Setting " + key + "->" + value)
    client.set(StringToChannelBuffer(key), StringToChannelBuffer(value))
    println("Getting value for key " + key)
    val getResult = Await.result(client.get(StringToChannelBuffer(key)))
    getResult match {
      case Some(n) => println("Got result: " + new String(n.array))
      case None => println("Didn't get the value!")
    }
  }

  def generateTestCases(): mutable.HashMap[String, mutable.HashMap[String, String]] = {
    val numSets = 100
    val setSize = 100

    val dict: mutable.HashMap[String, mutable.HashMap[String, String]] =
      new mutable.HashMap[String, mutable.HashMap[String, String]]

    for (i <- 0 until numSets) {
      val outerKey = UUID.randomUUID().toString
      val temp: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
      for (j <- 0 until setSize) {
        val innerKey = UUID.randomUUID().toString
        val value = UUID.randomUUID().toString
        temp.put(innerKey, value)
      }
      dict.put(outerKey, temp)
    }

    dict
  }

  def testBadd(client: Client, dict: mutable.HashMap[String, mutable.HashMap[String, String]]) {
    for ((outerKey, inner) <- dict) {
      for ((innerKey, value) <- inner) {
        val target = client.bAdd(StringToChannelBuffer(outerKey), StringToChannelBuffer(innerKey),
          StringToChannelBuffer(value))
        assert(Await.result(target) == 1, "BADD failed for " + outerKey + " " + innerKey)
      }
    }

    println("Test BADD succeeded")
  }

  def testBcard(client: Client, dict: mutable.HashMap[String, mutable.HashMap[String, String]]) {
    for ((outerKey, inner) <- dict) {
      val target = client.bCard(StringToChannelBuffer(outerKey))
      assert(inner.size == Await.result(target),
        "BCARD failed for " + outerKey + " expected " + inner.size + " got " + Await.result(target))
      }

      println("Test BCARD succeeded")
    }

  def testBget(client: Client, dict: mutable.HashMap[String, mutable.HashMap[String, String]]) {
    for ((outerKey, inner) <- dict) {
      for ((innerKey, value) <- inner) {
        val target = client.bGet(StringToChannelBuffer(outerKey), StringToChannelBuffer(innerKey))
        val targetVal = CBToString(Await.result(target).get)
        assert(value == targetVal,
          "BGET failed for " + outerKey + " expected " + value + " got " + targetVal)
      }
    }

    println("Test BGET succeeded")
  }

  def testBrem(client: Client, dict: mutable.HashMap[String, mutable.HashMap[String, String]]) {
    for ((outerKey, inner) <- dict) {
      for ((innerKey, value) <- inner) {
        val target = client.bRem(StringToChannelBuffer(outerKey),
          Seq(StringToChannelBuffer(innerKey)))
        assert(Await.result(target) == 1, "BREM failed for " + outerKey + " " + innerKey)
        inner.remove(innerKey)
      }
    }

    println("Test BREM succeeded")
  }

  def testBrange(client: Client, dict: mutable.HashMap[String, mutable.HashMap[String, String]]) {
    for ((outerKey, inner) <- dict) {
      val innerKeys = inner.toList.sortBy(_._1)
      val target = Await.result(client.bRange(StringToChannelBuffer(outerKey), None, None))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE succeeded")
  }

  def testBrangeInclusiveStart(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ) {
    val rand = new scala.util.Random()
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = rand.nextInt(innerKeys.size)
      innerKeys = innerKeys.drop(start)
      val target = Await.result(client.bRange(StringToChannelBuffer(outerKey),
        Option(StringToChannelBuffer(innerKeys.head._1)), None))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Inclusive Start succeeded")
  }

  def testBrangeInclusiveEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ) {
    val rand = new scala.util.Random()
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val end = rand.nextInt(innerKeys.size)
      innerKeys = innerKeys.dropRight(end)
      val target = Await.result(client.bRange(StringToChannelBuffer(outerKey), None,
        Option(StringToChannelBuffer(innerKeys.last._1))))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Inclusive End succeeded")
  }

  def testBrangeInclusiveStartEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ) {
    val rand = new scala.util.Random()
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = rand.nextInt(innerKeys.size)
      val end = rand.nextInt(innerKeys.size)
      val target = client.bRange(
        StringToChannelBuffer(outerKey),
        Option(StringToChannelBuffer(innerKeys(start)._1)),
        Option(StringToChannelBuffer(innerKeys(end)._1)))

      if (start > end) {
        assert(Await.ready(target).poll.get.isThrow,
          "BRANGE failed for " + outerKey + " return should be a throw")
      }
      else {
        innerKeys = innerKeys.slice(start, end + 1)
        validate(outerKey, innerKeys, Await.result(target))
      }
    }

    println("Test BRANGE Inclusive Start End succeeded")
  }

  def testBrangeExclusiveStart(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ) {
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = UUID.randomUUID().toString
      innerKeys = innerKeys.filter(p => (start <= p._1))
      val target = Await.result(client.bRange(StringToChannelBuffer(outerKey),
        Option(StringToChannelBuffer(start)), None))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Exclusive Start succeeded")
  }

  def testBrangeExclusiveEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ) {
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val end = UUID.randomUUID().toString
      innerKeys = innerKeys.filter(p => (p._1 <= end))
      val target = Await.result(client.bRange(StringToChannelBuffer(outerKey), None,
        Option(StringToChannelBuffer(end))))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Exclusive End succeeded")
  }

  def testBrangeExclusiveStartEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ) {
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = UUID.randomUUID().toString
      val end = UUID.randomUUID().toString
      innerKeys = innerKeys.filter(p => (start <= p._1 && p._1 <= end))
      val target = client.bRange(
        StringToChannelBuffer(outerKey),
        Option(StringToChannelBuffer(start)),
        Option(StringToChannelBuffer(end)))

      if (start > end) {
        assert(Await.ready(target).poll.get.isThrow,
          "BRANGE failed for " + outerKey + " return should be a throw")
      }
      else {
        validate(outerKey, innerKeys, Await.result(target))
      }
    }

    println("Test BRANGE Exclusive Start End succeeded")
  }

  def validate(
    outerKey: String,
    exp: List[(String, String)],
    got: Seq[(ChannelBuffer, ChannelBuffer)]
  ) {
    assert(got.size == exp.size,
      "BRANGE failed for " + outerKey + " expected size " + exp.size + " got size " + got.size)

    for (i <- 0 until exp.size) {
      val expKey = exp(i)._1
      val gotKey = CBToString(got(i)._1)
      val expVal = exp(i)._2
      val gotVal = CBToString(got(i)._2)
      assert(exp(i)._1 == CBToString(got(i)._1),
        "Key mismatch for outerKey " + outerKey + " expected " + expKey + "got " + gotKey)
      assert(exp(i)._2 == CBToString(got(i)._2),
        "Value mismatch for outerKey " + outerKey + " expected " + expVal + "got " + gotVal)
    }
  }
}
