package com.twitter.finagle.netty3

import org.jboss.netty.channel._

private[netty3] trait Refcounted[CF <: ChannelFactory] extends (() => CF) {
  private sealed trait Counted
  private object Empty extends Counted
  private case class Full(cf: CF, n: Int) extends Counted

  private[this] var state: Counted = Empty

  // Release `cf` in a thread. Netty3 does not allow
  // releaseExternalResources() to be called in an IO worker (this
  // can cause deadlocks) so we spawn an ephemeral thread to
  // shut it down.
  private[this] def release(cf: CF) {
    (new Thread {
      override def run() {
        cf.releaseExternalResources()
      }
    }).start()
  }

  protected val make: () => CF
  protected def newWrapper(f: CF): CF

  protected def releaseWrapper() = synchronized {
    state match {
      case Empty => assert(false)
      case Full(_, n) if n <= 0 => assert(false)
      case Full(cf, 1) =>
        release(cf)
        state = Empty
      case Full(cf, n) =>
        state = Full(cf, n-1)
    }
  }

  def apply(): CF = synchronized {
    state match {
      case Full(cf, n) =>
        state = Full(cf, n+1)
      case Empty =>
        state = Full(make(), 1)
    }

    val Full(cf, _) = state
    newWrapper(cf)
  }
}

/**
 * A shareable Netty3 client channel factory. Uses reference counting
 * to create and dispose of channel factories as needed.
 */
class NewChannelFactory(protected val make: () => ChannelFactory)
    extends Refcounted[ChannelFactory] {
  protected def newWrapper(f: ChannelFactory) = new ChannelFactory {
    def newChannel(pipeline: ChannelPipeline) = f.newChannel(pipeline)
    def releaseExternalResources() = releaseWrapper()
  }
}

/**
 * A shareable Netty3 server channel factory. Uses reference counting
 * to create and dispose of channel factories as needed.
 */
class NewServerChannelFactory(protected val make: () => ServerChannelFactory)
    extends Refcounted[ServerChannelFactory] {
  protected def newWrapper(f: ServerChannelFactory) = new ServerChannelFactory {
    def newChannel(pipeline: ChannelPipeline) = f.newChannel(pipeline)
    def releaseExternalResources() = releaseWrapper()
  }
}

