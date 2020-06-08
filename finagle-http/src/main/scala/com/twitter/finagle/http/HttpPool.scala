package com.twitter.finagle.http

import com.twitter.finagle.client.DefaultPool
import com.twitter.finagle.http2.transport.client.H2Pool
import com.twitter.finagle.pool.BalancingPool
import com.twitter.finagle.{ServiceFactory, Stack}

/**
 * HTTP pooling module
 *
 * A module that serves as indirection for the HTTP pooling strategy. Based on
 * the protocol version, and in the case of HTTP/2 the handshake semantics, the
 * correct pooling strategy is selected.
 */
private[finagle] object HttpPool extends Stack.Module[ServiceFactory[Request, Response]] {

  val role: Stack.Role = DefaultPool.Role

  val description: String = "Control HTTP client connection pool"

  val parameters: Seq[Stack.Param[_]] = Nil

  def make(
    params: Stack.Params,
    next: Stack[ServiceFactory[Request, Response]]
  ): Stack[ServiceFactory[Request, Response]] = {
    val poolModule =
      if (ClientEndpointer.isPriorKnowledgeEnabled(params))
        // For multiplexed prior knowledge we need to use the SingletonPool because we
        // can concurrently dispatch against the HTTP/2 service implementation
        // just like with Mux.
        BalancingPool.module[Request, Response](allowInterrupts = false)
      else if (ClientEndpointer.isHttp2Enabled(params))
        // If we're using the multiplex codec we need to be able to dynamically switch
        // between the DefaultPool and the singleton pool, and that indirection is
        // done in the H2Pool.
        H2Pool.module
      else
        // Otherwise, we should just use the standard pool module.
        DefaultPool.module[Request, Response]

    poolModule +: next
  }
}
