package com.twitter.finagle.redis.integration

import com.twitter.finagle.Redis
import com.twitter.finagle.redis.Client
import com.twitter.finagle.redis.util.BufToString
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import java.util.UUID
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable

final class BtreeClientIntegrationSuite extends AnyFunSuite with BeforeAndAfterAll {
  var client: Client = _
  var dict: mutable.HashMap[String, mutable.HashMap[String, String]] = _
  val TIMEOUT = Duration.fromSeconds(10)

  override def beforeAll(): Unit = {
    // Until the Redis Server Btree changes are checked in and merged into master
    // The redis server needs to be started and shutdown externally
    // And the client connects it via the port 6379
    // After the changes become a part of the installed redis server
    // This will use RedisCluster to start and manage the external redis server
    val hostAddress = "127.0.0.1:6379"
    client = Redis.newRichClient(hostAddress)
    dict = generateTestCases()
    assert(client != null)
  }

  override def afterAll(): Unit = {
    println("Closing client...")
    client.flushDB()
    client.close()
    println("Done!")
  }

  ignore("Correctly add outerkey, innerkey and value tuples using BADD command") {
    testBadd(client, dict)
  }

  ignore("Correctly return cardinality for outerkey using BCARD command") {
    testBcard(client, dict)
  }

  ignore("Correctly return value for outerkey, innerkey pair using BGET command") {
    testBget(client, dict)
  }

  ignore("Correctly return BRANGE from start to end for outerkey") {
    testBrange(client, dict)
  }

  ignore("Correctly return BRANGE from a start key that exists to the end for outerkey") {
    testBrangeInclusiveStart(client, dict)
  }

  ignore("Correctly return BRANGE from start to end key that exists for outerkey") {
    testBrangeInclusiveEnd(client, dict)
  }

  ignore("Correctly return BRANGE from start key to end key where both exist for outerkey") {
    testBrangeInclusiveStartEnd(client, dict)
  }

  ignore("Correctly return BRANGE from start key that doesn't exist to end for outerkey") {
    testBrangeExclusiveStart(client, dict)
  }

  ignore("Correctly return BRANGE from start to end key that doesn't exist for outerkey") {
    testBrangeExclusiveEnd(client, dict)
  }

  ignore("Correctly return BRANGE from start key to end key where both don't exist for outerkey") {
    testBrangeExclusiveStartEnd(client, dict)
  }

  ignore("Correctly return removal of innerkey value pairs for outerkey using BREM command") {
    testBrem(client, dict)
  }

  ignore("Correctly return cardinality function for outerkey using BCARD command") {
    testBcard(client, dict)
  }

  ignore("Test commands for a key that doesn't exist") {
    testCacheMissOnCommands(client, dict)
  }

  ignore("Correctly merge lkeys with destination and set expiry") {
    val bufFoo = Buf.Utf8("foo")
    val bufBoo = Buf.Utf8("boo")
    val bufBaz = Buf.Utf8("baz")
    val bufBar = Buf.Utf8("bar")
    val bufMoo = Buf.Utf8("moo")

    Await.result(client.bAdd(bufFoo, bufBaz, bufBar), TIMEOUT)
    Await.result(client.pExpire(bufFoo, 10000), TIMEOUT)
    var result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    var ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
    assert(result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar"))
    assert(ttl > 0 && ttl <= 10000)

    Await.result(client.bMergeEx(bufFoo, Map(bufBaz -> bufBoo, bufMoo -> bufBoo), 90000), TIMEOUT)
    result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
    assert(
      result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")
    ) //baz's value is unchanged
    assert(ttl > 10000 && ttl <= 90000) // ttl is updated only if a field was added
    Await.result(client.flushAll(), TIMEOUT) //clear the keys
  }

  ignore("Correctly merge lkeys with destination without expiry") {
    val bufFoo = Buf.Utf8("foo")
    val bufBoo = Buf.Utf8("boo")
    val bufBaz = Buf.Utf8("baz")
    val bufBar = Buf.Utf8("bar")
    val bufMoo = Buf.Utf8("moo")

    Await.result(client.bAdd(bufFoo, bufBaz, bufBar), TIMEOUT)
    var result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    assert(result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar"))

    //merge foo-> baz,boo  moo,boo
    Await.result(client.bMergeEx(bufFoo, Map(bufBaz -> bufBoo, bufMoo -> bufBoo), -1), TIMEOUT)
    result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    var ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
    assert(ttl == -1)
    assert(
      result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")
    ) //moo->boo added, baz's value is unchanged
    Await.result(client.flushAll(), TIMEOUT) //clear the keys
  }

  ignore("TTL updated on merge only if a field was added.") {
    val bufFoo = Buf.Utf8("foo")
    val bufBoo = Buf.Utf8("boo")
    val bufBaz = Buf.Utf8("baz")
    val bufBar = Buf.Utf8("bar")
    val bufMoo = Buf.Utf8("moo")

    //add foo -> baz, bar
    Await.result(client.bAdd(bufFoo, bufBaz, bufBar), TIMEOUT)
    Await.result(client.pExpire(bufFoo, 10000), TIMEOUT)
    var result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    var ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get

    assert(result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar"))
    assert(ttl > 0 && ttl <= 10000)

    //merge foo -> baz,moo  moo,boo
    Await.result(client.bMergeEx(bufFoo, Map(bufBaz -> bufMoo, bufMoo -> bufBoo), 30000), TIMEOUT)
    result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
    assert(
      result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")
    ) // only moo->boo is added
    assert(ttl > 10000 && ttl <= 30000) // ttl updated.

    //merge foo -> baz,bar  moo,bar
    Await.result(client.bMergeEx(bufFoo, Map(bufBaz -> bufBar, bufMoo -> bufBar), 30000), TIMEOUT)
    result = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT).toList
    ttl = Await.result(client.pTtl(bufFoo), TIMEOUT).get
    assert(
      result.map(t => t._2).map(Buf.Utf8.unapply).flatten == Seq("bar", "boo")
    ) // values not updated
    assert(ttl > 10000 && ttl <= 30000) // ttl was not updated updated.
    Await.result(client.flushAll(), TIMEOUT) //clear the keys
  }

  def defaultTest(client: Client): Unit = {
    val key = "megatron"
    val value = "optimus"

    println("Setting " + key + "->" + value)
    client.set(Buf.Utf8(key), Buf.Utf8(value))
    println("Getting value for key " + key)
    val getResult = Await.result(client.get(Buf.Utf8(key)))
    getResult match {
      case Some(n) => println("Got result: " + Buf.Utf8.unapply(n).get)
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

  def testBadd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      for ((innerKey, value) <- inner) {
        val target = client.bAdd(Buf.Utf8(outerKey), Buf.Utf8(innerKey), Buf.Utf8(value))
        assert(Await.result(target) == 1, "BADD failed for " + outerKey + " " + innerKey)
      }
    }

    println("Test BADD succeeded")
  }

  def testBcard(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      val target = client.bCard(Buf.Utf8(outerKey))
      assert(
        inner.size == Await.result(target),
        "BCARD failed for " + outerKey + " expected " + inner.size + " got " + Await.result(target)
      )
    }

    println("Test BCARD succeeded")
  }

  def testBget(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      for ((innerKey, value) <- inner) {
        val target = client.bGet(Buf.Utf8(outerKey), Buf.Utf8(innerKey))
        val targetVal = BufToString(Await.result(target).get)
        assert(
          value == targetVal,
          "BGET failed for " + outerKey + " expected " + value + " got " + targetVal
        )
      }
    }

    println("Test BGET succeeded")
  }

  def testBrem(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      for ((innerKey, value) <- inner) {
        val target = client.bRem(Buf.Utf8(outerKey), Seq(Buf.Utf8(innerKey)))
        assert(Await.result(target) == 1, "BREM failed for " + outerKey + " " + innerKey)
        inner.remove(innerKey)
      }
    }

    println("Test BREM succeeded")
  }

  def testBrange(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      val innerKeys = inner.toList.sortBy(_._1)
      val target = Await.result(client.bRange(Buf.Utf8(outerKey), innerKeys.size, None, None))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE succeeded")
  }

  def testBrangeInclusiveStart(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    val rand = new scala.util.Random()
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = rand.nextInt(innerKeys.size)
      innerKeys = innerKeys.drop(start)
      val target = Await.result(
        client
          .bRange(Buf.Utf8(outerKey), innerKeys.size, Option(Buf.Utf8(innerKeys.head._1)), None),
        TIMEOUT
      )
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Inclusive Start succeeded")
  }

  def testBrangeInclusiveEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    val rand = new scala.util.Random()
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val end = rand.nextInt(innerKeys.size)
      innerKeys = innerKeys.dropRight(end)
      val target = Await.result(
        client
          .bRange(Buf.Utf8(outerKey), innerKeys.size, None, Option(Buf.Utf8(innerKeys.last._1))),
        TIMEOUT
      )
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Inclusive End succeeded")
  }

  def testBrangeInclusiveStartEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    val rand = new scala.util.Random()
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = rand.nextInt(innerKeys.size)
      val end = rand.nextInt(innerKeys.size)
      val target = client.bRange(
        Buf.Utf8(outerKey),
        innerKeys.size,
        Option(Buf.Utf8(innerKeys(start)._1)),
        Option(Buf.Utf8(innerKeys(end)._1))
      )

      if (start > end) {
        assert(
          Await.ready(target).poll.get.isThrow,
          "BRANGE failed for " + outerKey + " return should be a throw"
        )
      } else {
        innerKeys = innerKeys.slice(start, end + 1)
        validate(outerKey, innerKeys, Await.result(target))
      }
    }

    println("Test BRANGE Inclusive Start End succeeded")
  }

  def testBrangeExclusiveStart(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = UUID.randomUUID().toString
      innerKeys = innerKeys.filter(p => (start <= p._1))
      val target = Await.result(
        client.bRange(Buf.Utf8(outerKey), innerKeys.size, Option(Buf.Utf8(start)), None)
      )
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Exclusive Start succeeded")
  }

  def testBrangeExclusiveEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val end = UUID.randomUUID().toString
      innerKeys = innerKeys.filter(p => (p._1 <= end))
      val target =
        Await.result(client.bRange(Buf.Utf8(outerKey), innerKeys.size, None, Option(Buf.Utf8(end))))
      validate(outerKey, innerKeys, target)
    }

    println("Test BRANGE Exclusive End succeeded")
  }

  def testBrangeExclusiveStartEnd(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    for ((outerKey, inner) <- dict) {
      var innerKeys = inner.toList.sortBy(_._1)
      val start = UUID.randomUUID().toString
      val end = UUID.randomUUID().toString
      innerKeys = innerKeys.filter(p => (start <= p._1 && p._1 <= end))
      val target = client.bRange(
        Buf.Utf8(outerKey),
        innerKeys.size,
        Option(Buf.Utf8(start)),
        Option(Buf.Utf8(end))
      )

      if (start > end) {
        assert(
          Await.ready(target).poll.get.isThrow,
          "BRANGE failed for " + outerKey + " return should be a throw"
        )
      } else {
        validate(outerKey, innerKeys, Await.result(target))
      }
    }

    println("Test BRANGE Exclusive Start End succeeded")
  }

  def testCacheMissOnCommands(
    client: Client,
    dict: mutable.HashMap[String, mutable.HashMap[String, String]]
  ): Unit = {
    val bufFoo = Buf.Utf8("foo")
    val bufBar = Buf.Utf8("bar")

    var rangeResult = Await.result(client.bRange(bufFoo, 10, None, None), TIMEOUT)
    assert(rangeResult.isEmpty)

    var remResult = Await.result(client.bRem(bufFoo, Seq(bufBar)), TIMEOUT)
    assert(remResult == 0)

    var cardResult = Await.result(client.bCard(bufFoo), TIMEOUT)
    assert(cardResult == 0)

    var getResult = Await.result(client.bGet(bufFoo, bufBar), TIMEOUT)
    assert(getResult == None)
  }

  def validate(outerKey: String, exp: List[(String, String)], got: Seq[(Buf, Buf)]): Unit = {
    assert(
      got.size == exp.size,
      "BRANGE failed for " + outerKey + " expected size " + exp.size + " got size " + got.size
    )

    for (i <- 0 until exp.size) {
      val expKey = exp(i)._1
      val gotKey = BufToString(got(i)._1)
      val expVal = exp(i)._2
      val gotVal = BufToString(got(i)._2)
      assert(
        exp(i)._1 == BufToString(got(i)._1),
        "Key mismatch for outerKey " + outerKey + " expected " + expKey + "got " + gotKey
      )
      assert(
        exp(i)._2 == BufToString(got(i)._2),
        "Value mismatch for outerKey " + outerKey + " expected " + expVal + "got " + gotVal
      )
    }
  }
}
