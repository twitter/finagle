package com.twitter.finagle.http2.exp.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.http2.DeadConnectionException
import com.twitter.finagle.http2.transport.ClientSession
import com.twitter.finagle.netty4.param.Allocator
import com.twitter.finagle.netty4.transport.ChannelTransport
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{CancelledConnectionException, Failure, FailureFlags, Stack, Status}
import com.twitter.logging.Level
import com.twitter.util.{Future, Promise, Time}
import io.netty.channel.{
  Channel,
  ChannelFuture,
  ChannelFutureListener,
  ChannelInitializer,
  ChannelOption
}
import io.netty.handler.codec.http2.{
  Http2MultiplexCodec,
  Http2StreamChannel,
  Http2StreamChannelBootstrap
}
import io.netty.util
import io.netty.util.concurrent.GenericFutureListener
import java.lang.{Boolean => JBool}

private final class ClientSessionImpl(
  params: Stack.Params,
  initializer: ChannelInitializer[Channel],
  channel: Channel)
    extends ClientSession {

  private[this] final class ChildTransport(ch: Channel)
      extends ChannelTransport(
        ch = ch,
        readQueue = new AsyncQueue[Any],
        omitStackTraceOnInactive = true
      ) {
    override def status: Status = {
      Status.worst(ClientSessionImpl.this.status, super.status)
    }
  }

  private[this] val codec: Http2MultiplexCodec = {
    val codec = channel.pipeline.get(classOf[Http2MultiplexCodec])
    if (codec == null) {
      throw new IllegalStateException(
        s"Parent Channel doesn't have an instance of ${classOf[Http2MultiplexCodec].getSimpleName}"
      )
    }
    codec
  }

  private[this] val bootstrap: Http2StreamChannelBootstrap =
    new Http2StreamChannelBootstrap(channel)
      .option(ChannelOption.ALLOCATOR, params[Allocator].allocator)
      .option[JBool](ChannelOption.AUTO_READ, false) // backpressure for streams
      .handler(initializer)

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

  def status: Status = {
    // Note that the result of `connection.goAwayReceived` doesn't have any guarantees
    // regarding memory visibility since the field that stores the value is not volatile.
    // However, since `status` is racy anyway we tolerate it as fixing it would be much
    // more complex.
    if (!channel.isOpen) Status.Closed
    else if (codec.connection.goAwayReceived) Status.Closed
    else Status.Open
  }

  /**
   * Construct a new `Transport` from a Netty4 stream channel.
   *
   * @note This method is only useful for handling the upgrade pathway since in that
   *       circumstance the `Http2MultiplexCodec` initializes the upgraded stream.
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
    val p = Promise[Transport[Any, Any]]
    val nettyFuture = bootstrap.open()

    p.setInterruptHandler {
      case _ =>
        nettyFuture.cancel( /*mayInterruptIfRunning*/ false)
    }

    // This is largely the same code as found in the ConnectionBuilder but doesn't
    // perform the second phase where we then make something out of the `Transport`.
    nettyFuture.addListener(new GenericFutureListener[util.concurrent.Future[Http2StreamChannel]] {
      def operationComplete(future: util.concurrent.Future[Http2StreamChannel]): Unit = {
        if (future.isCancelled) {
          p.setException(
            Failure(
              cause = new CancelledConnectionException,
              flags = FailureFlags.Interrupted | FailureFlags.Retryable,
              logLevel = Level.DEBUG
            )
          )
        } else if (!future.isSuccess) {
          p.setException(Failure.rejected(future.cause))
        } else {
          val channel = future.get
          val trans = new ChildTransport(channel)
          channel.connect(channel.remoteAddress)
          p.setValue(trans)
        }
      }
    })

    p
  }
}
