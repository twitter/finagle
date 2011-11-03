package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.util.{Duration,Time}
import collection._
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

case class OpenConnectionsThresholds(lowWaterMark: Int, highWaterMark: Int) {
  require(lowWaterMark <= highWaterMark, "lowWaterMark must be <= highWaterMark")
}

/*
* Trait defining a class responsible for managing IdleConnection
* This class is notified everytime a connection receive activity or when it is closed
* You also can retrieved from this class an idle connection.
*/
trait IdleConnectionHandler {
  def getIdleConnections: Iterable[Channel]
  def getIdleConnection: Option[Channel]
  def getIdleTimeout: Duration
  def countIdleConnections: Int = getIdleConnections.size
  def markChannelAsActive(channel: Channel): Unit
  def removeChannel(channel: Channel): Unit
}


/*
* Always return the most idle connection
*/
class PreciseIdleConnectionHandler(idleTimeout: Duration) extends IdleConnectionHandler {
  private[this] val activeConnections = mutable.HashMap.empty[Channel,Time]

  def getIdleTimeout = idleTimeout

  def getIdleConnections = synchronized {
    val now = Time.now
    activeConnections.view.filter{ case (_, ts) => idleTimeout < now - ts }.map(_._1)
  }

  def getIdleConnection: Option[Channel] = synchronized {
    val now = Time.now
    val idleConnections = activeConnections.filter{ case (_, ts) => idleTimeout < now - ts }
    if(idleConnections.isEmpty)
      None
    else
      Some(idleConnections.min(Ordering.by[(Channel,Time),Time]{ case (_, ts) => ts })._1)
  }

  def markChannelAsActive(channel: Channel) = synchronized {
    activeConnections += (channel -> Time.now)
  }

  def removeChannel(channel: Channel) = synchronized {
    activeConnections -= channel
  }
}


/*
 * Keep track of active connection using 3 buckets in a circular buffer way, every time some activity
 * happens on a connection, we move the channel from the corresponding bucket to the most recent one
 * So, if we need a idle connection, we just have to choose randomly from the oldest bucket
 * NB: This implementation doesn't guarantee that connections that have been idle during exactly
 * idleTimeout will be detected as idle, it may take at most 2 times idleTimeout to be detected
 */
class BucketIdleConnectionHandler(idleTimeout: Duration) extends IdleConnectionHandler {
  private[this] val bucketSize = idleTimeout.inMilliseconds
  private[this] val bucketNumber = 3
  private[this] val activeConnections = (1 to bucketNumber).map{ _ => mutable.HashSet.empty[Channel] }.toArray

  private[this] def currentBucketIndex = Time.now.inMilliseconds / bucketSize
  private[this] var lastBucketIndex = currentBucketIndex

  private[this] def getBucket(i: Int, currentNewIndex: Long) = synchronized {
    val index = currentNewIndex + i

    def move(fromIndex: Long , toIndex: Long) {
      activeConnections((toIndex % bucketNumber).toInt) ++=
        activeConnections((fromIndex % bucketNumber).toInt)
      activeConnections((fromIndex % bucketNumber).toInt).clear()
    }

    val res = (currentNewIndex - lastBucketIndex) match {
      case 0L =>
        activeConnections((index % bucketNumber).toInt)
      case 1L =>
        move(currentNewIndex, currentNewIndex + 1)
        lastBucketIndex = currentNewIndex
        activeConnections((index % bucketNumber).toInt)
      case _ =>
        move(currentNewIndex, currentNewIndex + 1)
        move(currentNewIndex - 1, currentNewIndex + 1)
        lastBucketIndex = currentNewIndex
        activeConnections((index % bucketNumber).toInt)
    }
    res
  }

  private[this] def newestBucket(currentNewIndex: Long) = getBucket(0, currentNewIndex)
  private[this] def intermediateBucket(currentNewIndex: Long) = getBucket(-1, currentNewIndex)
  private[this] def oldestBucket(currentNewIndex: Long) = getBucket(-2, currentNewIndex)

  def getIdleTimeout = idleTimeout

  def getIdleConnections: Iterable[Channel] = synchronized {
    val currentNewIndex = currentBucketIndex
    oldestBucket(currentNewIndex)
  }

  def getIdleConnection: Option[Channel] = synchronized {
    val currentNewIndex = currentBucketIndex
    oldestBucket(currentNewIndex).headOption
  }

  def markChannelAsActive(channel: Channel) = synchronized {
    val currentNewIndex = currentBucketIndex
    if(! oldestBucket(currentNewIndex).remove(channel))
      intermediateBucket(currentNewIndex).remove(channel)
    newestBucket(currentNewIndex).add(channel)
  }
  def removeChannel(channel: Channel) = synchronized {
    val currentNewIndex = currentBucketIndex
    if(! oldestBucket(currentNewIndex).remove(channel))
      if(! intermediateBucket(currentNewIndex).remove(channel))
        newestBucket(currentNewIndex).remove(channel)
  }
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
    val thresholds: OpenConnectionsThresholds,
    idleConnectionHandler: IdleConnectionHandler,
    val statsReceiver : StatsReceiver = NullStatsReceiver)
  extends SimpleChannelHandler
{
  private[this] val connectionCounter = new AtomicInteger(0)
  private[this] val refusedConnectionCounter = new AtomicInteger(0)
  private[this] val startDate = Time.now

  statsReceiver.addGauge("idle_connections_closed") { idleConnectionHandler.countIdleConnections }
  statsReceiver.addGauge("idle_active_connections_ratio") {
    idleConnectionHandler.countIdleConnections.toFloat / connectionCounter.get
  }
  statsReceiver.addGauge("connections_refused") { refusedConnectionCounter.get }
  statsReceiver.addGauge("connection_refused_sec") {
    refusedConnectionCounter.get / startDate.untilNow.inSeconds.toInt
  }

  def openConnections = connectionCounter.get()

  def closeIdleConnections() = {
    var hasClosedAConnection = false
    idleConnectionHandler.getIdleConnection.foreach{ c =>
      c.close()
      hasClosedAConnection = true
    }
    hasClosedAConnection
  }


  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    def accept() {
      idleConnectionHandler.markChannelAsActive(ctx.getChannel)
      ctx.sendUpstream(e)
    }

    val connectionCount = connectionCounter.incrementAndGet()
    if(connectionCount <= thresholds.lowWaterMark)
      accept()
    else if(connectionCount <= thresholds.highWaterMark) {
      closeIdleConnections()
      accept()
    }
    else {
      // Try to close idle connections, if we don't find any, then we refuse the connection
      if(closeIdleConnections())
        accept()
      else {
        refusedConnectionCounter.incrementAndGet()
        ctx.getChannel.close()
      }
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    idleConnectionHandler.removeChannel(ctx.getChannel)
    connectionCounter.decrementAndGet()
    super.channelClosed(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    idleConnectionHandler.markChannelAsActive(ctx.getChannel)
    super.messageReceived(ctx,e)
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    idleConnectionHandler.markChannelAsActive(ctx.getChannel)
    super.writeRequested(ctx,e)
  }
}
