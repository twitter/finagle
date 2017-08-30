package com.twitter.finagle.netty4.transport

import com.twitter.finagle.transport.TransportContext
import java.util.concurrent.Executor

private[finagle] trait HasExecutor { self: TransportContext =>

  /**
   * An `Executor` associated with this transport. If set, the executor must:
   *  1) Ensure serial execution ordering
   *  2) Run one item at a time
   *
   * For netty-based transports, a channel's EventLoop meets these requirements.
   */
  private[finagle] def executor: Executor
}
