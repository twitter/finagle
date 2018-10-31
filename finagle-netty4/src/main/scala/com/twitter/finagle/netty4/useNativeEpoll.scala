package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag

/**
 * When available, use Linux's native transport directly instead of bouncing through Java NIO.
 *
 * This flag exposes a programmable way of overriding a value via a `set` method. It could
 * be useful in tests where `Flag.let` is not applicable or introduces too much boilerplate.
 *
 * Example:
 *
 * {{{
 * class Test {
 *   @Before
 *   def before(): Unit = useNativeEpoll.set(false)
 *
 *   @After
 *   def after(): Unit = useNativeEpoll.reset()
 * }
 * }}}
 *
 * @see https://netty.io/wiki/native-transports.html
 */
private object useNativeEpoll
    extends GlobalFlag[Boolean](true, "Use Linux's native epoll transport, when available") {

  @volatile private var overrideValue = Option.empty[Boolean]

  protected override def getValue: Option[Boolean] = overrideValue match {
    case v @ Some(_) => v
    case _ => super.getValue
  }

  def set(value: Boolean): Unit = {
    overrideValue = Some(value)
  }

  override def reset(): Unit = {
    overrideValue = None
    super.reset()
  }
}
