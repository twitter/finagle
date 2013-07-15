package com.twitter.finagle.netty3

import com.twitter.util.Promise
import org.jboss.netty.channel._

/**
 * A ChannelFuture that doesn't need to have a channel on creation.
 */
private[finagle] class LatentChannelFuture extends DefaultChannelFuture(null, false) {
  @volatile private var channel: Channel = _

  def setChannel(c: Channel) { channel = c }
  override def getChannel() = channel
}

private[finagle] sealed abstract class State
private[finagle] case object Cancelled extends State
private[finagle] case class Ok(channel: Channel) extends State
private[finagle] case class Error(cause: Throwable) extends State

private[finagle] class CancelledException extends Exception

// TODO: decide what to do about cancellation here.
private[finagle] class RichChannelFuture(val self: ChannelFuture) {
  def apply(f: State => Unit) {
    self.addListener(new ChannelFutureListener {
      def operationComplete(future: ChannelFuture) {
        f(new RichChannelFuture(future).state)
      }
    })
  }

  def update(state: State) {
    state match {
      case Ok(channel)  => self.setSuccess()
      case Error(cause) => self.setFailure(cause)
      case Cancelled    => self.cancel()
    }
  }

  def state: State =
    if (self.isSuccess)
      Ok(self.getChannel)
    else if (self.isCancelled)
      Cancelled
    else
      Error(self.getCause)

  def proxyTo(other: ChannelFuture) {
    this {
      case Ok(channel)  => other.setSuccess()
      case Error(cause) => other.setFailure(cause)
      case Cancelled    => other.cancel()
    }
  }

  /**
   * the ChannelFuture forms a Monad.
   */
  def flatMap(f: Channel => ChannelFuture): ChannelFuture = {
    val future = new LatentChannelFuture

    // TODO: cancellation.

    this {
      case Ok(channel) =>
        val nextFuture = f(channel)
        nextFuture.addListener(new ChannelFutureListener {
          def operationComplete(nextFuture: ChannelFuture) {
            future.setChannel(nextFuture.getChannel)
            if (nextFuture.isSuccess)
              future.setSuccess()
            else
              future.setFailure(nextFuture.getCause)
          }
        })
      case Error(throwable) =>
        future.setFailure(throwable)
      case Cancelled =>
        future.cancel()
    }

    future
  }

  def map[T](f: Channel => Channel) = {
    val future = new LatentChannelFuture
    future.setChannel(self.getChannel)

    this {
      case Ok(channel) =>
        future.setChannel(f(channel))
        future.setSuccess()
      case Error(cause) =>
        future.setFailure(cause)
      case Cancelled =>
        future.cancel()
    }

    future
  }

  def foreach[T](f: Channel => T) {
    this {
      case Ok(channel) => f(channel)
      case _ => ()
    }
  }

  def onSuccess(f: => Unit) = {
    foreach { _ => f }
    self
  }

  def onError(f: Throwable => Unit) = {
    this {
      case Error(cause) => f(cause)
      case _ => ()
    }
    self
  }

  def onSuccessOrFailure(f: => Unit) {
    this {
      case Cancelled => ()
      case _ => f
    }
  }

  def onCancellation(f: => Unit) {
    this {
      case Cancelled => f
      case _ => ()
    }
  }

  // tbd
  // def always(f: Channel => Unit) =
  //   this { case state: State => f(self.getChannel) }

  def orElse(other: RichChannelFuture): ChannelFuture = {
    val combined = new LatentChannelFuture

    this.proxyTo(combined)
    other.proxyTo(combined)

    combined
  }

  def andThen(next: ChannelFuture): ChannelFuture = flatMap { _ => next }

  def close() {
    self.addListener(ChannelFutureListener.CLOSE)
  }

  def toTwitterFuture = {
    val result = new Promise[Unit]
    apply {
      case Ok(_)     => result.setValue(())
      case Cancelled => result.setException(new CancelledException)
      case Error(e)  => result.setException(e)
    }
    result
  }
}
