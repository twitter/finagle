package com.twitter.finagle.http2.transport

import com.twitter.logging.Logger
import com.twitter.util.{Time, Timer}
import io.netty.channel.{
  ChannelDuplexHandler,
  ChannelFuture,
  ChannelFutureListener,
  ChannelHandlerContext,
  ChannelPromise
}
import io.netty.handler.codec.http2.{
  DefaultHttp2GoAwayFrame,
  Http2ConnectionHandler,
  Http2Error,
  Http2Frame
}
import io.netty.util.ReferenceCountUtil
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A handler for managing some aspects of the HTTP/2 servers main pipeline
 *
 * This filter handles a number of session concerns that the `Http2MultiplexCodec`
 * exposes as pipeline events.
 *
 * - Swallows inbound H2 session frames that are propagated down the main pipeline.
 * - Swallows exceptions that are propagated down the pipeline such as GOAWAY exceptions.
 * - Manages graceful shutdown to allow servers to perform the preferred method of
 *   session draining where we process new streams that raced with the GOAWAY sent to
 *   the client.
 */
private[http2] final class H2Filter(timer: Timer) extends ChannelDuplexHandler {
  import H2Filter.logger

  @volatile private[this] var closeDeadline: Time = Time.Bottom

  /** Set the deadline for closing the connection */
  def setDeadline(deadline: Time): Unit = {
    closeDeadline = deadline
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case frame: Http2Frame =>
      ReferenceCountUtil.release(frame)
    case _ =>
      super.channelRead(ctx, msg)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Swallowed so as to not bork the parent pipeline. This includes
    // GOAWAY messages.
    ()
  }

  override def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    val connectionHandler = ctx.pipeline.get(classOf[Http2ConnectionHandler])

    if (connectionHandler == null) {
      // Illegal state.
      val message = s"Found H2Filter in a pipeline without a Http2ConnectionHandler. " +
        s"Pipeline: ${ctx.pipeline}"
      val ex = new IllegalStateException(message)
      logger.error(ex, message)
      ctx.close(promise)
    } else {
      val deadline = closeDeadline
      val now = Time.now

      if (deadline <= now) {
        // Since we're not sending our own GOAWAY we need to make sure that we set the
        // deadline in `Http2ConnectionHandler` since it try to do the non-graceful shutdown.
        connectionHandler.gracefulShutdownTimeoutMillis(0)
        logger.debug(s"Deadline already passed ($deadline <= $now). Closing now.")
        super.close(ctx, promise)
      } else {
        logger.info(s"Closing h2 session with deadline $deadline")
        // Send a GOAWAY frame and set a timer to forcefully close the channel.
        val goawayFrame = new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR)
        // This is gaming the system. Netty wants you to specify how many bonus racy streams
        // you're willing to handle and you can't just say the last stream id. So, what you do
        // is just say that you'll take more values than could possible be done and let Netty
        // cap it to Int.MaxValue for you. Not pretty, but effective.
        goawayFrame.setExtraStreamIds(Int.MaxValue)

        // We need to make sure we only utilize our promise once.
        val promiseOnce = new AtomicBoolean()

        // If we fail to write the frame we do a 'hard close' to make sure the channel goes down.
        ctx
          .writeAndFlush(goawayFrame).addListener(new ChannelFutureListener {
            def operationComplete(future: ChannelFuture): Unit =
              if (!future.isSuccess && promiseOnce.compareAndSet(false, true)) {
                ctx.close(promise)
              }
          })

        // Now that we've sent our GOAWAY, lets set a timer to force the thing closed by
        // the deadline if the other peer doesn't hang up on us first.
        val closeTask = timer.schedule(deadline) {
          if (promiseOnce.compareAndSet(false, true)) {
            ctx.channel.eventLoop.execute(new Runnable {
              def run(): Unit = {
                // Write a second GOAWAY with the last observed stream and then
                // close up shop.
                logger.info(
                  "Graceful draining period lapsed. " +
                    "Sending final GOAWAY and closing the connection."
                )
                ctx
                  .writeAndFlush(new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR))
                  .addListener(new ChannelFutureListener {
                    def operationComplete(future: ChannelFuture): Unit = ctx.close(promise)
                  })

              }
            })

          }
        }

        // Don't leave stuff laying around in the common case where the peer hangs up.
        ctx.channel.closeFuture.addListener(new ChannelFutureListener {
          def operationComplete(future: ChannelFuture): Unit = {
            closeTask.cancel()
            if (promiseOnce.compareAndSet(false, true)) {
              logger.info("Channel closed, session terminated.")
              promise.setSuccess()
            }
          }
        })
      }
    }
  }
}

private[http2] object H2Filter {
  private val logger = Logger.get(classOf[H2Filter])

  val HandlerName: String = "H2Filter"
}
