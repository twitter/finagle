package com.twitter.finagle.server

import com.twitter.finagle.{Server, Stack}

/**  A [[com.twitter.finagle.Server Server]] that is parametrized. */
trait StackBasedServer[Req, Rep]
    extends Server[Req, Rep]
    with Stack.Parameterized[StackBasedServer[Req, Rep]]
    with Stack.Transformable[StackBasedServer[Req, Rep]]
