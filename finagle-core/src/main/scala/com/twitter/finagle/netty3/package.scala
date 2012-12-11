package com.twitter.finagle

/**
 * Package netty3 implements the bottom finagle primitives:
 * {{com.twitter.finagle.Server}} and a client transport in terms of
 * the netty3 event loop.
 *
 * Note: when {{com.twitter.finagle.builder.ClientBuilder}} and
 * {{com.twitter.finagle.builder.ServerBuilder}} are deprecated,
 * package netty3 can move into its own package, so that only the
 * (new-style) clients and servers that depend on netty3 bring it in.
 */
package object netty3
