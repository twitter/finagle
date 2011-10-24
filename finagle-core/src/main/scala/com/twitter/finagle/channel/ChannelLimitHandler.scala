package com.twitter.finagle.channel

import org.jboss.netty.channel._
import com.twitter.util.Duration

case class OpenConnectionsThresholds(lowWaterMark: Int, highWaterMark: Int) {
  require(lowWaterMark <= highWaterMark, "lowWaterMark must be <= highWaterMark")
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
class ChannelLimitHandler(thresholds: OpenConnectionsThresholds, idleTimeoutDuration : Duration, nbOfConnectionToClose : Int = 5)
    extends SimpleChannelHandler
    with LifeCycleAwareChannelHandler
{
  import collection._

  private[this] var connectionCount = 0
  private[this] val activeConnections = mutable.HashMap.empty[Channel,Long]
  private[this] val idleTimeout = idleTimeoutDuration.inMilliseconds

  def openConnections = connectionCount

  def closeIdleConnections( maxNbOfConnectionToClose : Int ) = {
    synchronized {
      val now = System.currentTimeMillis()

      // Sort idle connections (connection that haven't received/send msg since idleTimeout ms) by their last activity date
      val idleConnectionsSortedByLastActivityDate =
        activeConnections.foldLeft( immutable.TreeMap.empty[Long,Channel] ){
          case (treemap,(channel,lastActivityDate)) =>
            if( idleTimeout < now - lastActivityDate )
              treemap + (lastActivityDate -> channel)
            else
              treemap
        }

      idleConnectionsSortedByLastActivityDate.values.take( maxNbOfConnectionToClose ).foreach( _.close )

      // return the number of connections that we have actually closed
      math.min( maxNbOfConnectionToClose , idleConnectionsSortedByLastActivityDate.size )
    }
  }

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    synchronized {
      connectionCount += 1
      if( thresholds.highWaterMark < connectionCount ) {
        // Try to close idle connections, if we don't find any (return 0), then we refuse the connection
        if( 0 == closeIdleConnections(nbOfConnectionToClose) )
          ctx.getChannel.close()
      }
      else {
        val now = System.currentTimeMillis()
        if( thresholds.lowWaterMark < connectionCount ){
          closeIdleConnections( nbOfConnectionToClose )
          activeConnections += (ctx.getChannel -> now)
        }
        else
          activeConnections += (ctx.getChannel -> now)
      }
    }
    super.channelOpen(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    synchronized {
      connectionCount -= 1
      activeConnections -= ctx.getChannel
    }

    super.channelClosed(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    synchronized {
      activeConnections.update( ctx.getChannel , System.currentTimeMillis() )
    }
    super.messageReceived(ctx,e)
  }

  override def beforeAdd(ctx: ChannelHandlerContext)    {/*nop*/}
  override def afterAdd(ctx: ChannelHandlerContext)     {/*nop*/}
  override def beforeRemove(ctx: ChannelHandlerContext) {/*nop*/}
  override def afterRemove(ctx: ChannelHandlerContext)  {/*nop*/}
}
