package com.twitter.finagle.http2.transport.server

import com.twitter.logging.Logger
import com.twitter.util.{Time, Timer}
import io.netty.channel.{
  Channel,
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
import io.netty.util.{AttributeKey, ReferenceCountUtil}

/**
 * A handler for managing some aspects of the HTTP/2 Server main pipeline
 *
 * This filter handles a number of session concerns that the `Http2MultiplexHandler`
 * exposes as pipeline events.
 *
 * - Swallows inbound H2 session frames that are propagated down the main pipeline.
 * - Turn exceptions into channel close calls since the tail can no longer close the pipeline.
 * - Manages graceful shutdown to allow servers to perform the preferred method of
 *   session draining where we process new streams that raced with the GOAWAY sent to
 *   the client.
 *
 * @see [[com.twitter.finagle.http2.transport.client.H2ClientFilter]] for clients
 */
private[http2] final class H2ServerFilter(timer: Timer, channel: Channel)
    extends ChannelDuplexHandler {
  import H2ServerFilter.logger

  // We need to make sure we only close the channel once. Close calls only
  // happen from within the event loop so we don't need synchronization.
  private[this] var ctx: ChannelHandlerContext = null
  private[this] var closeCalled = false
  private[this] val channelClosePromise = channel.newPromise

  // We shuttle all channel close calls through this method to make sure
  // we only send the close event one time.
  private[this] def onceCloseChannel(ctx: ChannelHandlerContext): Unit = {
    if (!closeCalled) {
      closeCalled = true
      ctx.close(channelClosePromise)
    }
  }

  override def handlerAdded(ctx: ChannelHandlerContext): Unit = {
    assert(this.ctx == null)

    this.ctx = ctx
    // Make sure this channel hasn't already been asked to close. This circumvents
    // a race where a channel is opened, the server thinks its H1 and asks it to shutdown
    // but before those events can propagate we upgrade to H2 and swallow the close calls.
    val shutdownDeadline = ctx.channel.attr(H2ServerFilter.CloseRequestAttribute).get
    if (shutdownDeadline != null) {
      // Looks like a close was called so initiate the shutdown pathway.
      gracefulShutdown(shutdownDeadline)
    }
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = msg match {
    case frame: Http2Frame =>
      ReferenceCountUtil.release(frame)
    case _ =>
      super.channelRead(ctx, msg)
  }

  override def close(ctx: ChannelHandlerContext, promise: ChannelPromise): Unit = {
    // We use a different pathway for shutting down so we swallow the close request.
    channelClosePromise.addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture): Unit = {
        if (future.isSuccess) promise.setSuccess()
        else promise.setFailure(future.cause)
      }
    })
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    super.exceptionCaught(ctx, cause)
    // Handlers further down the pipeline cannot close the pipeline and there
    // shouldn't be any way to recover from this, we will close things down
    // after pitching it down further.
    onceCloseChannel(ctx)
  }

  def gracefulShutdown(deadline: Time): Unit = {
    assert(ctx.executor.inEventLoop)
    val connectionHandler = ctx.pipeline.get(classOf[Http2ConnectionHandler])

    if (connectionHandler == null) {
      // Illegal state.
      val message = s"Found H2ServerFilter in a pipeline without a Http2ConnectionHandler. " +
        s"Pipeline: ${ctx.pipeline}"
      val ex = new IllegalStateException(message)
      logger.error(ex, message)
      onceCloseChannel(ctx)
    } else {
      val now = Time.now

      if (deadline <= now) {
        // Since we're not sending our own GOAWAY we need to make sure that we set the
        // deadline in `Http2ConnectionHandler` since it try to do the non-graceful shutdown.
        connectionHandler.gracefulShutdownTimeoutMillis(0)
        logger.ifDebug(s"Deadline already passed ($deadline <= $now). Closing now.")
        onceCloseChannel(ctx)
      } else {
        logger.ifDebug(s"Closing h2 session with deadline $deadline")
        // Send a GOAWAY frame and set a timer to forcefully close the channel.
        val goawayFrame = new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR)
        // This is gaming the system. Netty wants you to specify how many bonus racy streams
        // you're willing to handle and you can't just say the last stream id. So, what you do
        // is just say that you'll take more values than could possible be done and let Netty
        // cap it to Int.MaxValue for you. Not pretty, but effective.
        goawayFrame.setExtraStreamIds(Int.MaxValue)

        // If we fail to write the frame we do a 'hard close' to make sure the channel goes down.
        ctx
          .writeAndFlush(goawayFrame).addListener(new ChannelFutureListener {
            def operationComplete(future: ChannelFuture): Unit =
              if (!future.isSuccess) {
                onceCloseChannel(ctx)
              }
          })

        // Now that we've sent our GOAWAY, lets set a timer to force the thing closed by
        // the deadline if the other peer doesn't hang up on us first.
        val closeTask = timer.schedule(deadline) {
          ctx.channel.eventLoop.execute(new Runnable {
            def run(): Unit = {
              if (closeCalled) {
                logger.debug("Close task found close event already called")
              } else {
                // Write a second GOAWAY with the last observed stream and then
                // close up shop.
                logger.debug(
                  "Graceful draining period lapsed. " +
                    "Sending final GOAWAY and closing the connection."
                )
                ctx
                  .writeAndFlush(new DefaultHttp2GoAwayFrame(Http2Error.NO_ERROR))
                  .addListener(new ChannelFutureListener {
                    def operationComplete(future: ChannelFuture): Unit = {
                      onceCloseChannel(ctx)
                    }
                  })
              }
            }
          })
        }

        // Don't leave stuff laying around in the common case where the peer hangs up.
        ctx.channel.closeFuture.addListener(new ChannelFutureListener {
          def operationComplete(future: ChannelFuture): Unit = {
            closeTask.cancel()
          }
        })
      }
    }
  }
}

private[http2] object H2ServerFilter {
  private val logger = Logger.get(classOf[H2ServerFilter])

  val HandlerName: String = "H2ServerFilter"

  /**
   * Channel attachment used to signal that a close request has happened
   *
   * The purpose of this is to prevent race conditions where a close call is
   * sent but the channel isn't yet HTTP/2 so the `H2ServerFilter` close pathway
   * isn't used, yet the channel then immediately upgrades to HTTP/2 and the
   * standard close pathway is defeated because the `H2ServerFilter` is now
   * swallowing close calls.
   */
  val CloseRequestAttribute = AttributeKey.newInstance[Time]("h2serverfilter-close-time")
}
