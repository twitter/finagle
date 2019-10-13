package com.twitter.finagle.serverset2

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.twitter.cache.EvictingCache
import com.twitter.cache.caffeine.LoadingFutureCache
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.logging.{Level, Logger}
import com.twitter.util.Future
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/**
 * A cache for the members and vectors of a given cluster (/twitter/service/role/env/job).
 *
 * This cache effectively caches from a full ZK member/vector path, to a Seq[Entry] or Seq[Vector]
 *
 * It's a bit counter intuitive that we go from path to Seq[Entity], not path to Entity.
 *
 * In the case of Entries, one path, such as /twitter/service/a/b/c/member_01, will have as data
 * json similar to the following:
 * {
 *   "status": "ALIVE",
 *   "additionalEndpoints":
 *   {
 *     "http": {"host": "smf1-dad-25-sr1.prod.twitter.com", "port": 31623},
 *     "aurora": {"host": "smf1-dad-25-sr1.prod.twitter.com", "port": 31863},
 *     "health": {"host": "smf1-dad-25-sr1.prod.twitter.com", "port": 31863}
 *   },
 *   "serviceEndpoint":
 *   {
 *     "host": "smf1-dad-25-sr1.prod.twitter.com", "port": 31623
 *   },
 *   "shard": 7
 * }
 *
 * This translates into multiple endpoints (which are Entries), one per unique host:port pair. In the above JSON:
 *
 * Seq(
 *  Endpoint(Seq("aurora", "health"), "smf1-dad-25-sr1.prod.twitter.com", 31863, ...)
 *  Endpoint(Seq(null, "http"), "smf1-dad-25-sr1.prod.twitter.com", 31623, ...)
 * )
 *
 * In Vector's case this Seq[Vector] will always be of length 1.
 *
 *
 * The cache is an EvictingCache, which evicts failed Futures. It's based on a LoadingFutureCache,
 * based on a LoadingCache, backed by a ConcurrentMap. This is the recommended FutureCache to use
 * in util-cache.
 *
 * We access invalidation via the LoadingCache, and key-enumeration via the underlying ConcurrentMap,
 * (accessed via asMap) thus we need to keep instances of each of these underlying classes.
 */
private[serverset2] abstract class ZkNodeDataCache[Entity](
  clusterPath: String,
  entityType: String,
  statsReceiver: StatsReceiver) {
  private[this] val logger = Logger(getClass)

  /** zkSession needs to be set via setSession before the cache is used */
  private[this] val zkSession = new AtomicReference(ZkSession.nil)

  protected[this] def loadEntity(path: String): Future[Seq[Entity]] =
    zkSession.get.immutableDataOf(clusterPath + "/" + path).map {
      case Some(Buf.Utf8(data)) =>
        val results = parseNode(path, data)
        // avoid string concatenation cost if not logging debug
        if (logger.isLoggable(Level.DEBUG)) {
          logger.debug(s"$path retrieved ${results.length} for ${entityType}")
        }
        results
      case None => Nil
    }

  private[this] val underlying: LoadingCache[String, Future[Seq[Entity]]] =
    Caffeine
      .newBuilder()
      .build(
        new CacheLoader[String, Future[Seq[Entity]]] {
          override def load(path: String): Future[Seq[Entity]] = loadEntity(path)
        }
      )

  private[this] val asMap = underlying.asMap

  private[this] val entityCache = EvictingCache.lazily(new LoadingFutureCache(underlying))

  private[this] val gauge = statsReceiver.addGauge(s"numberOf${entityType}Nodes") {
    underlying.estimatedSize
  }

  protected def parseNode(path: String, data: String): Seq[Entity]

  /** Get the nodes we have cached as an immutable scala set.  */
  def keys: Set[String] = asMap.keySet.asScala.toSet

  /**
   * Get the Entry/Vector for a node from cache, or fetch it from ZK via getData if it's not cached.
   * We create a new interruptible future on each call to get in order to avoid cancelling the
   * cached Future when a consumer of the cache cancels.
   */
  def get(memberNode: String): Future[Seq[Entity]] = entityCache.get(memberNode).get.interruptible()

  def remove(memberNode: String): Unit = underlying.invalidate(memberNode)

  def setSession(newZkSession: ZkSession): Unit = zkSession.set(newZkSession)
}

/** See [[ZkNodeDataCache]] comment */
private[serverset2] class ZkEntryCache(clusterPath: String, statsReceiver: StatsReceiver)
    extends ZkNodeDataCache[Entry](clusterPath, "Entry", statsReceiver) {
  override def parseNode(path: String, data: String): Seq[Entry] = Entry.parseJson(path, data)
}

/** See [[ZkNodeDataCache]] comment */
private[serverset2] class ZkVectorCache(clusterPath: String, statsReceiver: StatsReceiver)
    extends ZkNodeDataCache[Vector](clusterPath, "Vector", statsReceiver) {
  override def parseNode(path: String, data: String): Seq[Vector] = Vector.parseJson(data) match {
    case Some(vector) => Seq(vector)
    case _ => Nil
  }
}
