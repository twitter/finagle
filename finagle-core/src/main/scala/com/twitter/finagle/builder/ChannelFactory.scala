package com.twitter.finagle.builder

import org.jboss.netty.channel.{
  ChannelPipeline, ChannelFactory, ServerChannelFactory,
  ServerChannel}

class ReferenceCountedChannelFactory(underlying: ChannelFactory)
  extends ChannelFactory
{
  private[this] var refcount = 0

  def acquire() = synchronized {
    refcount += 1
  }

  def newChannel(pipeline: ChannelPipeline) = underlying.newChannel(pipeline)

  // TODO: after releasing external resources, we can still use the
  // underlying factory?  (ie. it'll create new threads, etc.?)
  def releaseExternalResources() = synchronized {
    refcount -= 1
    if (refcount <= 0)
      underlying.releaseExternalResources()
  }
}

/**
 * A "revivable" and lazy ChannelFactory that allows us to revive
 * ChannelFactories after they have been released.
 */
class LazyRevivableChannelFactory(make: () => ChannelFactory)
  extends ChannelFactory
{
  @volatile private[this] var underlying: ChannelFactory = null

  def newChannel(pipeline: ChannelPipeline) = {
    if (underlying eq null) synchronized {
      if (underlying eq null)
        underlying = make()
    }

    underlying.newChannel(pipeline)
  }

  def releaseExternalResources() = synchronized {
    var thread: Thread = null

    if (underlying ne null) {
      // releaseExternalResources must be called in a non-Netty
      // thread, otherwise it can lead to a deadlock.
      val _underlying = underlying
      thread = new Thread {
        override def run = {
          _underlying.releaseExternalResources()
        }
      }
      thread.start()
      underlying = null
    }
  }
}

/**
 * Yikes. This monstrosity is needed to wrap server channel factories
 * generically. Netty only checks the actual channel factory types for
 * servers dynamically, so using a ChannelFactory in the server
 * results in a runtime error. However, the above wrappers are generic
 * and should be shared among the different types of factories. We are
 * in effect exchanging the possibility of one runtime error for
 * another. Another approach would be to use dynamic proxies, but this
 * seems a little simpler for such a simple interface.
 */
class ChannelFactoryToServerChannelFactory(underlying: ChannelFactory)
  extends ServerChannelFactory
{
  def newChannel(pipeline: ChannelPipeline) =
    underlying.newChannel(pipeline).asInstanceOf[ServerChannel]
  def releaseExternalResources() =
    underlying.releaseExternalResources()
}
