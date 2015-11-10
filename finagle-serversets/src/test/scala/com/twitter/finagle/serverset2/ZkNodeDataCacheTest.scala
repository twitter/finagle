package com.twitter.finagle.serverset2

import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.Future
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ZkNodeDataCacheTest extends FunSuite {

  def nilZkSession = () => ZkSession.nil

  class ZkTestCache(
    clusterPath: String
  ) extends ZkNodeDataCache[String](clusterPath, "Test", NullStatsReceiver) {
    var parseNodeCalledCount = 0
    override def loadEntity(path: String) = {
      parseNodeCalledCount += 1
      Future.value(Seq("a", "b"))
    }
    override def parseNode(path: String, data: String) = Seq.empty
  }

  private[this] def newCache = new ZkTestCache("/stuff")

  test("ZkNodeDataCache#keys") {
    val cache = newCache
    cache.get("key1")
    assert(cache.keys == Set("key1"))
  }

  test("ZkNodeDataCache#remove") {
    val cache = newCache
    cache.get("key1")
    assert(cache.keys == Set("key1"))
    cache.remove("key1")
    assert(cache.keys == Set())
  }

  test("ZkNodeDataCache#get") {
    val cache = newCache
    cache.get("key1")
    assert(cache.keys == Set("key1"))
    assert(cache.parseNodeCalledCount == 1)
    cache.get("key1")
    assert(cache.keys == Set("key1"))
    assert(cache.parseNodeCalledCount == 1)
  }
}

