package com.twitter.finagle.context

class LocalContextTest extends AbstractContextTest {
  val ctx = new LocalContext
  val a = new ctx.Key[String]
  val b = new ctx.Key[Int]
}
