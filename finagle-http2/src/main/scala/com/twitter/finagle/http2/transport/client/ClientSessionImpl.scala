package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.http2.DeadConnectionException
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, FailureFlags, Stack, Status}
import com.twitter.util.{Future, Promise, Time}
import io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelInitializer,
  ChannelOption
}
import io.netty.handler.codec.http2.{
  Http2FrameCodec,
  Http2StreamChannel,
  Http2StreamChannelBootstrap
}
import io.netty.util
import io.netty.util.concurrent.GenericFutureListener
import java.lang.{Boolean => JBool}
import scala.util.control.NonFatal

private final class ClientSessionImpl(
  params: Stack.Params,
  initializer: ChannelInitializer[Channel],
  channel: Channel,
  failureDetectorStatus: () => Status)
    extends ClientSession {

  import ClientSessionImpl.StreamHighWaterMark

  // For the client we want to consider the status of the session.
  private[this] final class ChildTransport(ch: Channel) extends StreamChannelTransport(ch) {

    override def status: Status = {
      Status.worst(ClientSessionImpl.this.status, super.status)
    }
  }

  private[this] val codec: Http2FrameCodec = {
    val codec = channel.pipeline.get(classOf[Http2FrameCodec])
    if (codec == null) {
      throw new IllegalStateException(
        s"Parent Channel doesn't have an instance of ${classOf[Http2FrameCodec].getSimpleName}"
      )
    }
    codec
  }

  private[this] val bootstrap: Http2StreamChannelBootstrap = {
    // Note that we need to invert the boolean since auto read means no backpressure.
    val streamAutoRead = !params[Netty4Transporter.Backpressure].backpressure
    new Http2StreamChannelBootstrap(channel)
      .option(ChannelOption.ALLOCATOR, params[Allocator].allocator)
      .option[JBool](ChannelOption.AUTO_READ, streamAutoRead)
      .handler(initializer)
  }

  // Thread safety provided by synchronization on `this`
  private[this] var closeInitiated: Boolean = false
  private[this] val closeP = Promise[Unit]()

  def close(deadline: Time): Future[Unit] = {
    // We only want to initiate the close a single time to avoid sending
    // multiple close commands down the Netty pipeline.
    val doClose = this.synchronized {
      if (closeInitiated) false
      else {
        closeInitiated = true
        true
      }
    }

    if (doClose) {
      if (!channel.isOpen) closeP.setDone()
      else {
        channel
          .close().addListener(new ChannelFutureListener {
            def operationComplete(future: ChannelFuture): Unit = {
              if (future.isSuccess) closeP.setDone()
              else closeP.setException(future.cause)
            }
          })
      }
    }

    closeP
  }

  private[this] def sessionStatus: Status = {
    // Note that the result of `connection.goAwayReceived` doesn't have any guarantees
    // regarding memory visibility since the field that stores the value is not volatile.
    // However, since `status` is racy anyway we tolerate it as fixing it would be much
    // more complex.
    if (!channel.isOpen) Status.Closed
    // We're nearly out of stream ID's so signal closed so that the pooling layers will
    // shut us down and start up a new session.
    else if (codec.connection.local.lastStreamCreated > StreamHighWaterMark) Status.Closed
    // If we've received a GOAWAY frame we shouldn't attempt to open any new streams.
    else if (codec.connection.goAwayReceived) Status.Closed
    // If we can't open a stream that means that the maximum number of outstanding
    // streams already exists and thus we are busy.
    else if (!codec.connection.local.canOpenStream) Status.Busy
    else Status.Open
  }

  // Note that the probes of the connection instance are not thread safe because
  // Netty expects all operations to happen within the channels executor but since
  // Status is racy anyway, it should be good enough.
  def status: Status = Status.worst(failureDetectorStatus(), sessionStatus)

  /**
   * Construct a new `Transport` from a Netty4 stream channel.
   *
   * @note This method is only useful for handling the upgrade pathway since in that
   *       circumstance the `Http2MultiplexHandler` initializes the upgraded stream.
   */
  def newChildTransport(streamChannel: Channel): Transport[Any, Any] =
    new ChildTransport(streamChannel)

  def newChildTransport(): Future[Transport[Any, Any]] = {
    if (status != Status.Closed) initNewNettyChildChannel()
    else {
      val ex = new DeadConnectionException(
        channel.remoteAddress,
        FailureFlags.Retryable | FailureFlags.Rejected
      )
      Future.exception(ex)
    }
  }

  private[this] def initNewNettyChildChannel(): Future[Transport[Any, Any]] = {
    // This is similar to what is found in the ConnectionBuilder but doesn't
    // perform the second phase where we then make something out of the `Transport`
    // or wire up interrupts. We're not concerned about interrupts because the
    // operation should be very quick, just registering with the event loop, and thus
    // most often a waste of allocations.

    val p = Promise[Transport[Any, Any]]

    // We execute these checks in the channels event loop because otherwise we can expect
    // racy behavior from the different lookups from the codec state.
    try channel.eventLoop.execute { () =>
      if (!codec.connection.local.canOpenStream) {
        // The stream cannot be created because the max active streams are maxed out
        p.setException(
          Failure.rejected(
            "Unable to open stream because his session has the maximum number of active streams allowed: " +
              codec.connection.local.maxActiveStreams))
      } else if (codec.connection.goAwayReceived) {
        p.setException(Failure.rejected("Unable to open stream because the session is draining."))
      } else {
        // We should be good to go.
        bootstrap
          .open()
          .addListener(new GenericFutureListener[util.concurrent.Future[Http2StreamChannel]] {
            def operationComplete(future: util.concurrent.Future[Http2StreamChannel]): Unit = {
              if (!future.isSuccess) {
                p.setException(Failure.rejected(future.cause))
              } else {
                val channel = future.get
                val trans = new ChildTransport(channel)
                p.setValue(trans)
              }
            }
          })
//        }
      }
    } catch {
      case NonFatal(t) => p.setException(Failure.rejected(t))
    }

    p
  }
}

private object ClientSessionImpl {

  // The max stream id is the maximum possible 31-bit unsigned integer. We want to
  // close before that to avoid races so we've arbitrarily picked 50 remaining
  // streams (client initiated stream id's are odd numbered so we multiply by 2) as
  // the high water mark before we signal that this session is closed for business.
  private val StreamHighWaterMark: Int = Int.MaxValue - 100
}
