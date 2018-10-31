package com.twitter.finagle.client

import com.twitter.finagle.{Client, Stack}

/**
 * A [[com.twitter.finagle.Client Client]] that may have its
 * [[com.twitter.finagle.Stack Stack]] transformed.
 *
 * A `StackBasedClient` is weaker than a `StackClient` in that the
 * specific `Req`, `Rep` types of its stack are not exposed.
 */
trait StackBasedClient[Req, Rep]
    extends Client[Req, Rep]
    with Stack.Parameterized[StackBasedClient[Req, Rep]]
    with Stack.Transformable[StackBasedClient[Req, Rep]]
