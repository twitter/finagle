package com.twitter.finagle.serverset2

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class ZkNodeDataCacheTest extends AnyFunSuite {

  def nilZkSession = () => ZkSession.nil

  class ZkTestCache(clusterPath: String)
      extends ZkNodeDataCache[String](clusterPath, "Test", NullStatsReceiver) {
    var parseNodeCalledCount = 0
    var shouldThrow = false
    override def loadEntity(path: String) = {
      parseNodeCalledCount += 1
      if (shouldThrow) Future.exception(new Exception) else Future.value(Seq("a", "b"))
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

  test("ZkNodeDataCache#throw should not cache") {
    val cache = newCache
    cache.shouldThrow = true
    cache.get("key1")
    assert(cache.keys.isEmpty)
    cache.shouldThrow = false
    cache.get("key1")
    assert(cache.keys == Set("key1"))
    assert(cache.parseNodeCalledCount == 2)
  }
}
