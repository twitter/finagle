package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.util.{Duration,Time}
import com.twitter.conversions.time._
import collection._
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

case class OpenConnectionsThresholds(
  lowWaterMark: Int,
  highWaterMark: Int,
  idleTimeout: Duration
) {
  require(lowWaterMark <= highWaterMark, "lowWaterMark must be <= highWaterMark")
}

abstract class GenerationalQueue[A] {
  def touch(a: A)
  def add(a: A)
  def remove(a: A)
  def collect(d: Duration): Option[A]
  def collectAll(d: Duration): Iterable[A]
}

/**
 * Generational Queue keep track of elements based on their last activity.
 * You can refresh activity of an element by calling touch(a: A) on it.
 * There is 2 ways of retrieving old elements:
 * - collect(age: Duration) collect the oldest element (age of elem must be > age)
 * - collectAll(age: Duration) collect all the elements which age > age in parameter
 */
class ExactGenerationalQueue[A] extends GenerationalQueue[A] {
  private[this] val container = mutable.HashMap.empty[A, Time]
  private[this] implicit val ordering = Ordering.by[(A, Time), Time]{ case (_, ts) => ts }

  /**
   * touch insert the element if it is not yet present
   */
  def touch(a: A) = synchronized { container.update(a, Time.now) }

  def add(a: A) = synchronized { container += ((a, Time.now)) }

  def remove(a: A) = synchronized { container.remove(a) }

  def collect(age: Duration): Option[A] = synchronized {
    if (container.isEmpty)
      None
    else
      container.min match {
        case (a, t) if (age < Time.now - t) => Some(a)
        case _ => None
      }
  }

  def collectAll(age: Duration): Iterable[A] = synchronized {
    (container filter { case (_, t) => age < Time.now - t }).keys
  }
}


/**
 * Improved GenerationalQueue: using a list of buckets responsible for containing elements belonging
 * to a slice of time.
 * For instance: 3 Buckets, First contains elements from 0 to 10, second elements from 11 to 20
 * and third elements from 21 to 30
 * We expand the list when we need a new bucket, and compact the list to stash all old buckets
 * into one.
 * There is a slightly difference with the other implementation, when we collect elements we only
 * choose randomly an element in the oldest bucket, as we don't have activity date in this bucket
 * we consider the worst case and then we can miss some expired elements by never find elements
 * that aren't expired.
 */
class BucketGenerationalQueue[A](timeout: Duration) extends GenerationalQueue[A]
{
  object TimeBucket {
    def empty[A] = new TimeBucket[A](Time.now, timeSlice)
  }
  class TimeBucket[A](val origin: Time, var timeSize: Duration) extends mutable.HashSet[A] {
    override def toString() = "TimeBucket(origin=%d, size=%d, Set=%s)".format(
      origin.inMilliseconds, timeSize.inMilliseconds, super.toString()
    )
  }

  private[this] val timeSlice = timeout / 3
  private[this] var buckets = List(TimeBucket.empty[A])

  private[this] def extendChainIfNeeded() = {
    val now = Time.now
    val headBucket = buckets.head
    if (headBucket.origin + headBucket.timeSize < now) {
      buckets = TimeBucket.empty[A] :: buckets
      true
    }
    else
      false
  }

  private[this] def compactChain() = {
    val now = Time.now

    def accumulate(chain: List[TimeBucket[A]],
      accumulatedTime: Duration,
      newChain: List[TimeBucket[A]] = Nil,
      oldestBucket: TimeBucket[A] = TimeBucket.empty[A]
    ): List[TimeBucket[A]] = {
      if (chain.isEmpty)
        if (oldestBucket.isEmpty)
          newChain.reverse
        else
          (oldestBucket :: newChain).reverse
      else {
        val bucket = chain.head
        val newAccumulatedTime = accumulatedTime + (now - bucket.origin)
        if (newAccumulatedTime < timeout)
          accumulate(chain.tail, newAccumulatedTime, bucket :: newChain, oldestBucket)
        else {
          val newOldestBucket = oldestBucket
          newOldestBucket ++= bucket
          accumulate(chain.tail, newAccumulatedTime, newChain, newOldestBucket)
        }
      }
    }

    accumulate(buckets, 0.millisecond)
  }

  def updateBuckets() {
    if (extendChainIfNeeded())
      buckets = compactChain()
  }

  def touch(a: A) = synchronized {
    buckets.drop(1) foreach { _.remove(a) }
    add(a)
  }

  def add(a: A) = synchronized {
    updateBuckets()
    buckets.head.add(a)
  }

  def remove(a: A) = synchronized {
    buckets foreach { _.remove(a) }
    compactChain()
  }

  def collect(d: Duration): Option[A] = synchronized {
    val oldestBucket = buckets.last
    val ageOfNewestElement = oldestBucket.origin + oldestBucket.timeSize
    if (ageOfNewestElement < Time.now - d)
      oldestBucket.headOption
    else
      None
  }

  def collectAll(d: Duration): Iterable[A] = synchronized {
    def loop(bucketChain: List[TimeBucket[A]], accumulatedTime: Duration,
      result: Set[A] = Set.empty[A]
    ): Set[A] = {
      if (bucketChain.isEmpty)
        result
      else {
        val now = Time.now
        val bucket = bucketChain.head
        val newAccumulatedTime = accumulatedTime + (now - bucket.origin)
          if (d < newAccumulatedTime)
            loop(bucketChain.tail, newAccumulatedTime, result ++ bucket)
          else
            loop(bucketChain.tail, newAccumulatedTime, result)
      }
    }
    loop(buckets, 0.millisecond)
  }
}


/**
 * Trait defining a class responsible for managing IdleConnection
 * This class is notified everytime a connection receive activity or when it is closed
 * You also can retrieved from this class an idle connection.
 */
class IdleConnectionHandler(idleTimeout: Duration, queue: GenerationalQueue[Channel]) {
  def countIdleConnections: Int = getIdleConnections.size
  def getIdleTimeout = idleTimeout
  def getIdleConnections: Iterable[Channel] = queue.collectAll(idleTimeout)
  def getIdleConnection: Option[Channel] = queue.collect(idleTimeout)
  def addConnection(c: Channel) = queue.add(c)
  def markConnectionAsActive(c: Channel) = queue.touch(c)
  def removeConnection(c: Channel) = queue.remove(c)
}

/**
 * This Handler limit the number of connections of a server and try to close idle ones when the
 * server become overloaded.
 * Each time a new connection arrive:
 * - if below low watermark: accept the connection.
 * - if above low watermark: collect (close) idle connections, but accept the connection.
 * - if above high watermark: collect (close) idle connections, and refuse/accept the
 *   connection depending if we managed to close an idle connection.
 */
class ChannelLimitHandler(
    thresholds: OpenConnectionsThresholds,
    idleConnectionHandler: IdleConnectionHandler,
    statsReceiver : StatsReceiver = NullStatsReceiver)
  extends SimpleChannelHandler
{
  private[this] val connectionCounter = new AtomicInteger(0)
  private[this] val closedGauge = statsReceiver.addGauge("idle_connections_closed") {
    idleConnectionHandler.countIdleConnections
  }
  private[this] val ratioGauge = statsReceiver.addGauge("idle_active_connections_ratio") {
    idleConnectionHandler.countIdleConnections.toFloat / connectionCounter.get
  }
  private[this] val refusedConnectionCounter = statsReceiver.counter("connections_refused")

  def openConnections = connectionCounter.get()

  def closeIdleConnections() =
    idleConnectionHandler.getIdleConnection match {
      case Some(conn) =>
        conn.close(); true
      case None =>
        false
    }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val connectionCount = connectionCounter.incrementAndGet()
    val accept = if (connectionCount <= thresholds.lowWaterMark)
      true
    else if (connectionCount <= thresholds.highWaterMark) {
      closeIdleConnections()
      true
    } else {
      // Try to close idle connections, if we don't find any, then we refuse the connection
      if (closeIdleConnections())
        true
      else {
        refusedConnectionCounter.incr()
        false
      }
    }

    if (accept) {
      // We don't track this new connection in the idleConnectionManager, we wait that the server
      // responds first before tracking idle time
      super.channelOpen(ctx, e)
    } else {
      ctx.getChannel.close()
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    idleConnectionHandler.removeConnection(ctx.getChannel)
    connectionCounter.decrementAndGet()
    super.channelClosed(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    // Don't track connection until the server respond
    idleConnectionHandler.removeConnection(ctx.getChannel)
    super.messageReceived(ctx,e)
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    idleConnectionHandler.markConnectionAsActive(ctx.getChannel)
    super.writeRequested(ctx,e)
  }
}
