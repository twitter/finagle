package com.twitter.finagle.context

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LocalContextTest extends AbstractContextTest {
  val ctx = new LocalContext
  val a = new ctx.Key[String]
  val b = new ctx.Key[Int]
}