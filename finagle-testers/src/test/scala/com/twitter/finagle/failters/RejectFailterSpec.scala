package com.twitter.finagle.failters

import com.twitter.finagle.Service
import com.twitter.util.{Await, Var, Future}
import java.util.concurrent.RejectedExecutionException
import org.scalatest.matchers.MustMatchers
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.mockito.Mockito._


@RunWith(classOf[JUnitRunner])
case class RejectFailterSpec() extends FlatSpec with MustMatchers {
  behavior of "RejectFailters"

  val repeatFor = 10000

  it should "not fail with probability 0" in {
    val base = mock(classOf[Service[String, String]])

    when(base.apply("hello")).thenReturn(Future.value("hi"))
    val stack = RejectFailter(Var(0.0)) andThen base
    1 to repeatFor foreach { _ =>
      Await.result(stack("hello")) must equal ("hi")
    }
    verify(base, times(repeatFor)).apply("hello")
  }

  it should "not fail with probability 0 in byzantine" in {
    val base = mock(classOf[Service[String, String]])

    when(base.apply("hello")).thenReturn(Future.value("hi"))
    val stack = ByzantineRejectFailter(Var(0.0)) andThen base
    1 to repeatFor foreach { _ =>
      Await.result(stack("hello")) must equal ("hi")
    }
    verify(base, times(repeatFor)).apply("hello")
  }

  it should "fail 0.5 of the time in byzantine mode" in {
    val base = mock(classOf[Service[String, String]])

    when(base.apply("hello")).thenReturn(Future.value("hi"))
    val stack = ByzantineRejectFailter(Var(0.5)) andThen base

    var fail = 0
    var pass = 0

    try {
      Await.result(Future.collect(1 to repeatFor map { _ =>
        stack("hello") onSuccess { _ => pass += 1 } onFailure { _ => fail += 1 }
      }))
    } catch {
      case r : RejectedExecutionException => // Ignore
    }

    val failRatio = fail.toDouble / repeatFor.toDouble
    failRatio must be (0.5 plusOrMinus 0.05)
    // Verify that the service was called the number of required times
    verify(base, times(repeatFor)).apply("hello")
  }

  it should "fail roughly half the time" in {
    val base = mock(classOf[Service[String, String]])

    when(base.apply("hello")).thenReturn(Future.value("hi"))
    val stack = RejectFailter(Var(0.5)) andThen base

    var fail = 0
    var pass = 0

    1 to repeatFor foreach { _ =>
      try {
        Await.result(stack("hello") onSuccess { _ => pass += 1 } onFailure { _ => fail += 1 })
      } catch {
        case t : RejectedExecutionException => // Ignore
      }
    }
    val failRatio = fail.toDouble / repeatFor.toDouble
    failRatio must be (0.5 plusOrMinus 0.05)
    verify(base, times(pass)).apply("hello")
  }

}
