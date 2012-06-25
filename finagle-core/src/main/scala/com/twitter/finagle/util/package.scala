package com.twitter.finagle

/**
  * ==Resource Management==
  *
  * [[com.twitter.finagle.util.Disposable]] represents a live resource that must be disposed after use.
  * [[com.twitter.finagle.util.Managed]] is a factory for creating and composing such resources.
  * Managed resources are composed together so their lifetimes are synchronized.
  *
  * The following example shows how to build and use composite managed resources:
  *
  * {{{
  * // Create managed Tracer
  * def mkManagedTracer() = new Managed[Tracer] {
  *   def make() = new Disposable[Tracer] {
  *     val underlying = new Tracer()
  *     def get = underlying
  *     def dispose(deadline: Time) = underlying.release() // assumes Tracer uses relese() to manage lifetime
  *   }
  * }
  *
  * // Create managed Server using Tracer as dependency
  * def mkManagedServer(t: Tracer) = new Managed[Server] {
  *   def make() = new Disposable[Server] {
  *     val underlying = new Server(t) // Server requires tracer to be created
  *     def get = underlying
  *     def dispose(deadline: Time) = underlying.close() // assumes Server uses close() to manage lifetime
  *   }
  * }
  *
  * // Create composite resource
  * val compRes: Managed[Server] = for {
  *   a <- mkManagedTracer()
  *   b <- mkManagedServer(a)
  * } yield b
  *
  * // Use composite resource in safe fashion. It's guaranteed that both resources
  * // will be properly closed/released when done using them.
  * compRes foreach { b =>
  *   // use b (which is type Server in this case)
  * } // dispose called on both resources
  * }}}
  *
  * =Disposable/Managed Semantics=
  *
  * [[com.twitter.finagle.util.Disposable]]: get can be called multiple times and should return same instance;
  * dispose can be called only once and should release resource that get is returning;
  * calling get after dispose is undefined.
  *
  * [[com.twitter.finagle.util.Managed]]: multiple calls to make could return
  *  a) new instance or
  *  b) ref counted instance of the underlying resource or
  *  c) same instance when resource doesn't need to be actually disposed, etc.
  */
package object util