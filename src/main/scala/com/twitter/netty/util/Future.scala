package com.twitter.netty.util

import org.jboss.netty.channel._

/**
 * A ChannelFuture that doesn't need to have a channel on creation.
 */
class LatentChannelFuture extends DefaultChannelFuture(null, false) {
  var channel: Channel = _

  def setSuccess(c: Channel) { setChannel(c); super.setSuccess() }
  def setChannel(c: Channel) { channel = c }
  override def getChannel() = channel
}

object Ok {
  def unapply(f: ChannelFuture) = if (f.isSuccess) Some(f.getChannel) else None
}

object Error {
  def unapply(f: ChannelFuture) = if (f.isSuccess) None else Some(f.getCause)
}

class RichChannelFuture(val self: ChannelFuture) extends Proxy {
  def apply(f: ChannelFuture => Unit) {
    if (self.isDone) {
      f(self)
    } else {
      self.addListener(new ChannelFutureListener {
        def operationComplete(future: ChannelFuture) { f(future) }
      })
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
            if (nextFuture.isSuccess) {
              future.setSuccess(nextFuture.getChannel)
            } else {
              future.setFailure(nextFuture.getCause)
            }
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
        future.setSuccess(f(channel))
      case Error(throwable) =>
        future.setFailure(throwable)
    }

    future
  }

  def foreach[T](f: Channel => T) {
    apply {
      case Ok(channel) => f(channel)
      case _ => ()
    }
  }

  def close() {
    self.addListener(ChannelFutureListener.CLOSE)
 }
}

object Conversions {
  implicit def channelFutureToRichChannelFuture(f: ChannelFuture) =
    new RichChannelFuture(f)
}