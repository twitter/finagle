package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.util.Duration
import collection._
import java.util.concurrent.atomic.AtomicInteger

case class OpenConnectionsThresholds(lowWaterMark: Int, highWaterMark: Int) {
  require(lowWaterMark <= highWaterMark, "lowWaterMark must be <= highWaterMark")
}

trait IdleConnectionHandler {
  def getIdleConnection : Option[Channel]
  def markChannelAsActive( channel : Channel ) : Unit
  def removeChannel( channel : Channel ) : Unit
  def idleTimeout : Duration
}


/*
* Always return the most idle connection
*/
trait PreciseIdleConnectionHandler extends IdleConnectionHandler {
  private[this] val activeConnections = mutable.HashMap.empty[Channel,Long]

  def getIdleConnection : Option[Channel] = activeConnections synchronized {
    val now = System.currentTimeMillis()

    var oldestChannel : Option[Channel] = None
    var oldestActivityDate = Long.MaxValue
    for( (channel,lastActivityDate) <- activeConnections ) {
      if( lastActivityDate < oldestActivityDate && idleTimeout.inMillis < now - lastActivityDate ) {
          oldestChannel = Some(channel)
          oldestActivityDate = lastActivityDate
      }
    }
    oldestChannel
  }

  def markChannelAsActive( channel : Channel ) = activeConnections synchronized {
    activeConnections += (channel -> System.currentTimeMillis())
  }

  def removeChannel( channel : Channel ) = activeConnections synchronized {
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
trait BucketIdleConnectionHandler extends IdleConnectionHandler {
  private[this] val bucketSize = idleTimeout.inMilliseconds
  private[this] val bucketNumber = 3
  private[this] val activeConnections = (1 to bucketNumber).map{ _ => mutable.HashSet.empty[Channel] }

  private[this] def currentBucketIndex = (System.currentTimeMillis() / bucketSize).toInt
  private[this] var lastBucketIndex = currentBucketIndex

  private[this] def getBucket(i : Int, currentNewIndex : Int) = synchronized {
    val index = currentNewIndex + i

    def move( fromIndex : Int , toIndex : Int ) {
      activeConnections( toIndex % bucketNumber ) ++= activeConnections(fromIndex % bucketNumber)
      activeConnections( fromIndex % bucketNumber ).clear()
    }

    val res = (currentNewIndex - lastBucketIndex) match {
      case 0 =>
        activeConnections(index % bucketNumber)
      case 1 =>
        move(currentNewIndex, currentNewIndex + 1)
        lastBucketIndex = currentNewIndex
        activeConnections(index % bucketNumber)
      case _ =>
        move(currentNewIndex, currentNewIndex + 1)
        move(currentNewIndex - 1, currentNewIndex + 1)
        lastBucketIndex = currentNewIndex
        activeConnections(index % bucketNumber)
    }
    res
  }

  private[this] def newestBucket(currentNewIndex : Int) = getBucket(0, currentNewIndex)
  private[this] def intermediateBucket(currentNewIndex : Int) = getBucket(-1, currentNewIndex)
  private[this] def oldestBucket(currentNewIndex : Int) = getBucket(-2, currentNewIndex)

  def getIdleConnection : Option[Channel] = synchronized {
    val currentNewIndex = currentBucketIndex
    oldestBucket(currentNewIndex).headOption // orElse intermediateBucket(currentNewIndex).headOption
  }

  def markChannelAsActive( channel : Channel ) = synchronized {
    val currentNewIndex = currentBucketIndex
    (! oldestBucket(currentNewIndex).remove(channel) ) && (! intermediateBucket(currentNewIndex).remove(channel) )
    newestBucket(currentNewIndex).add( channel )
  }
  def removeChannel( channel : Channel ) = synchronized {
    val currentNewIndex = currentBucketIndex
    (! oldestBucket(currentNewIndex).remove(channel) ) && (! intermediateBucket(currentNewIndex).remove(channel) ) && (! newestBucket(currentNewIndex).remove(channel) )
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
class ChannelLimitHandler(val thresholds: OpenConnectionsThresholds, idleTimeoutDuration : Duration)
    extends SimpleChannelHandler
    with LifeCycleAwareChannelHandler
{
  this : IdleConnectionHandler =>

  private val connectionCounter = new AtomicInteger(0)
  override val idleTimeout = idleTimeoutDuration

  def openConnections = connectionCounter.get()

  def closeIdleConnections() = getIdleConnection.map( _.close() )

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val connectionCount = connectionCounter.incrementAndGet()
    if( thresholds.highWaterMark < connectionCount ) {
      // Try to close idle connections, if we don't find any, then we refuse the connection
      if( closeIdleConnections().isEmpty )
        ctx.getChannel.close()
    }
    else {
      if( thresholds.lowWaterMark < connectionCount ){
        closeIdleConnections()
        markChannelAsActive(ctx.getChannel)
      }
      else
        markChannelAsActive(ctx.getChannel)
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    removeChannel(ctx.getChannel)
    connectionCounter.decrementAndGet()

    super.channelClosed(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    markChannelAsActive(ctx.getChannel)
    super.messageReceived(ctx,e)
  }

  override def beforeAdd(ctx: ChannelHandlerContext)    {/*nop*/}
  override def afterAdd(ctx: ChannelHandlerContext)     {/*nop*/}
  override def beforeRemove(ctx: ChannelHandlerContext) {/*nop*/}
  override def afterRemove(ctx: ChannelHandlerContext)  {/*nop*/}
}

