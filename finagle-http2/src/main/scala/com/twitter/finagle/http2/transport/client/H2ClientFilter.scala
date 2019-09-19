package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.param.Stats
import com.twitter.finagle.{ChannelException, FailureFlags, Stack, Status}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise}
import io.netty.channel.{
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelInboundHandlerAdapter
}
import io.netty.handler.codec.http2.{DefaultHttp2PingFrame, Http2Frame, Http2PingFrame}
import io.netty.util.ReferenceCountUtil

/**
 * A filter for managing some aspects of an HTTP/2 Client pipeline.
 * This filter provides two primary functions:
 *   - A failure detection mechanism via pings for use on a shared/multiplexed
 *     [[ClientSession]]
 *   - Swallows inbound H2 session frames that are propagated down the main pipeline.
 *     (the Http2 -> Http1 object conversion should take place prior to this filter).
 *
 * @see [[com.twitter.finagle.http2.transport.server.H2ServerFilter]] for servers
 */
private[transport] final class H2ClientFilter(params: Stack.Params)
    extends ChannelInboundHandlerAdapter {
  import H2ClientFilter._

  /* mutable state, thread-safety is provided via the Netty pipeline EventLoop */
  private[this] var pingPromise: Promise[Unit] = null

  @volatile private[this] var ctx: ChannelHandlerContext = null

  private[this] val failureDetector: FailureDetector = {
    val statsReceiver = params[Stats].statsReceiver
    val detectorConfig = params[FailureDetector.Param].param
    FailureDetector(detectorConfig, ping _, statsReceiver.scope("failuredetector"))
  }

  // exposed for testing
  // The `falureDetector` will issue pings via this method. The ping promise is written out to the
  // Netty pipeline, but the resulting `pingPromise` is only considered successful when a Ping ACK
  // is received - which only occurs within a `channelRead`.
  def ping(): Future[Unit] =
    if (ctx == null) {
      Future.Done
    } else {
      val done = new Promise[Unit]
      ctx.channel
        .eventLoop().execute(new Runnable {
          def run(): Unit = {
            if (pingPromise == null) {
              pingPromise = done
              val nettyFuture = ctx.writeAndFlush(Ping)
              nettyFuture.addListener(new ChannelFutureListener {
                // this method tells us that we have successfully SENT the ping,
                // but only that it has been sent (NOT received an ACK)
                def operationComplete(f: ChannelFuture): Unit = {
                  if (pingPromise == null) {
                    log.debug(
                      "Got unmatched PING message when attempting " +
                        "to send a PING for address %s",
                      ctx.channel.remoteAddress)
                  } else if (!f.isSuccess) {
                    pingPromise.setException(ChannelException(f.cause, ctx.channel.remoteAddress))
                    pingPromise = null
                  }
                }
              })
            } else {
              done.setException(new PingOutstandingFailure)
            }
          }
        })
      done
    }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    this.ctx = ctx
    super.handlerAdded(ctx)
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case ping: Http2PingFrame if ping.ack() =>
        // we have received a Ping ACK, which should map to our `pingPromise`
        if (pingPromise == null) {
          log.debug("Got unmatched PING message for address %s", ctx.channel.remoteAddress)
        } else {
          pingPromise.setDone()
          pingPromise = null
        }
        ReferenceCountUtil.release(ping)
      case frame: Http2Frame =>
        // release any HTTP/2 Frames from propagating further down the pipeline
        ReferenceCountUtil.release(frame)
      case _ =>
        // send any non-HTTP/2 Frames down the pipeline
        super.channelRead(ctx, msg)
    }
  }

  def status: Status = failureDetector.status

}

private[transport] object H2ClientFilter {
  private val log: Logger = Logger.get(classOf[H2ClientFilter])

  case class PingOutstandingFailure(val flags: Long = FailureFlags.NonRetryable)
      extends Exception("A ping is already outstanding on this session.")
      with FailureFlags[PingOutstandingFailure] {
    protected def copyWithFlags(flags: Long): PingOutstandingFailure =
      copy(flags = flags)
  }

  val HandlerName: String = "PingDetectionHandler"

  // We currently re-use the same ping content and only verify that we receive an ack
  // on the same connection to simplify the state of what the filter needs to track, as
  // there can be multiple outstanding pings on the connection, which will cause the
  // `FailureDetector` to kick in. TODO - look into using/validating unique content.
  val Ping: Http2PingFrame = new DefaultHttp2PingFrame(0L)
}
