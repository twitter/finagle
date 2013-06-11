package com.twitter.finagle.benchmark

import com.twitter.finagle.Service
import com.twitter.finagle.tracing._
import com.twitter.util.{Local, Future}
import com.google.caliper.SimpleBenchmark
import com.twitter.finagle.zipkin.thrift.ZipkinTracer
import com.twitter.finagle.tracing.Annotation.{ServerSend, ServerRecv, Message}
import java.net.InetSocketAddress
import scala.util.Random
import com.twitter.finagle.stats.StatsReceiver

// From $BIRDCAGE_HOME run:
// ./bin/caliper finagle/finagle-benchmark com.twitter.finagle.benchmark.IsTracingBenchmark
class IsTracingBenchmark extends SimpleBenchmark {
  val tracer = ZipkinTracer.mk(sampleRate=0.0f)

  trait TraceState
  case class State(id: Option[TraceId], terminal: Boolean, tracers: List[Tracer]) extends TraceState
  case object NoState extends TraceState
  val rng = new Random
  val traceId = TraceId(Some(SpanId(123)), Some(SpanId(325)), SpanId(rng.nextLong()), Some(false), Flags())
  val local = new Local[State]
  var tracingEnabled = true

  def set(f: State => State) {
    local() match {
      case None    => local() = f(State(Some(traceId), false, Nil))
      case Some(s) => local() = f(s.copy(id = Some(traceId), terminal = false))
    }
  }

  def origisActivelyTracing: Boolean = {
    if (!tracingEnabled) false else { // short circuit
      local() match {
        case None => false
        case Some(State(_, _, tracers)) if tracers.isEmpty || tracers == List(NullTracer) => false
        case Some(State(Some(TraceId(_, _, _, isTraced, flags)), _, _)) if !flags.isDebug && isTraced == Some(false) => false
        case _ => true // backwards compat: default to trace on incomplete data
      }
    }
  }

  def ifStyle: Boolean = {
    val l = local()
    if (l == None || !tracingEnabled) false
    else {
      val state = l.get
      if (state.tracers.isEmpty || state.tracers == List(NullTracer))
        false
      else {
        val traceId = state.id
        if (traceId.isDefined) {
          val id = traceId.get
          id.sampled != Some(false)
        }
        else true
      }
    }
  }

  def caseStyle: Boolean = {
    if (!tracingEnabled) false else { // short circuit
      local() match {
        case Some(State(Some(TraceId(_, _, _, Some(false), Flags(0L))), _, _)) => false
        case None => false
        case Some(State(_, _, Nil)) => false
        case Some(State(_, _, List(NullTracer))) => false
        case Some(State(Some(TraceId(_, _, _, _, Flags(Flags.Debug))), _, _)) => true
        case _ => true // backwards compat: default to trace on incomplete data
      }
    }
  }

  set(state => state.copy(id=Some(traceId), tracers = List(tracer), terminal=false))

  def test(n: Int, f: => Boolean) {
    var i = 0
    while (i < n) {
      var j = 0
      while (j < 10000) {
        f
        j += 1
      }
      i += 1
    }
  }


  def timeOrig(n: Int) {
    test(n, origisActivelyTracing)
  }

  def timeIf(n: Int) {
    test(n, ifStyle)
  }

  def timeCase(n: Int) {
    test(n, caseStyle)
  }
}