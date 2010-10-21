package com.twitter.finagle.util

import org.jboss.netty.channel._

/**
 * A ChannelFuture that doesn't need to have a channel on creation.
 */
class LatentChannelFuture extends DefaultChannelFuture(null, false) {
  @volatile private var channel: Channel = _

  def setChannel(c: Channel) { channel = c }
  override def getChannel() = channel
}

object Ok {
  def unapply(f: ChannelFuture) = if (f.isSuccess) Some(f.getChannel) else None
}

object Error {
  def unapply(f: ChannelFuture) = if (f.isSuccess) None else Some(f.getCause)
}

object Error_ {
  def unapply(f: ChannelFuture) = if (f.isSuccess) None else Some(f.getCause, f.getChannel)
}

class RichChannelFuture(val self: ChannelFuture) {
  def apply(f: ChannelFuture => Unit) {
    if (self.isDone) {
      f(self)
    } else {
      self.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) { f(future) }
      })
    }
  }

  def proxyTo(other: ChannelFuture) {
    apply {
      case Ok(channel)  => other.setSuccess()
      case Error(cause) => other.setFailure(cause)
    }
  }

  /**
   * the ChannelFuture forms a Monad.
   */
  def flatMap(f: Channel => ChannelFuture): ChannelFuture = {
    val future = new LatentChannelFuture

    apply {
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
    }

    future
  }

  def map[T](f: Channel => Channel) = {
    val future = new LatentChannelFuture

    apply {
      case Ok(channel) =>
        future.setChannel(f(channel))
        future.setSuccess()
      case Error_(cause, channel) =>
        future.setChannel(channel)
        future.setFailure(cause)
    }

    future
  }

  def foreach[T](f: Channel => T) {
    apply {
      case Ok(channel) => f(channel)
      case _ => ()
    }
  }

  def onError(f: Throwable => Unit) {
    apply {
      case Error(cause) => f(cause)
      case Ok(_) => ()
    }
  }

  def always(f: Channel => Unit) =
    apply { case future => f(future.getChannel) }

  def close() {
    self.addListener(ChannelFutureListener.CLOSE)
 }
}

object Conversions {
  implicit def channelFutureToRichChannelFuture(f: ChannelFuture) =
    new RichChannelFuture(f)
}
