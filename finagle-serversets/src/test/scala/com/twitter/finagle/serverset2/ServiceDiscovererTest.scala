package com.twitter.finagle.serverset2

import com.twitter.finagle.serverset2.ZkOp.{GetData, GetChildrenWatch, ExistsWatch}
import com.twitter.finagle.serverset2.client.{Node, Data, WatchState, Watched}
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.io.Buf
import com.twitter.util._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ServiceDiscovererTest extends FunSuite {
  def ep(port: Int) = Endpoint(Array(null), "localhost", port, Int.MinValue, Endpoint.Status.Alive, port.toString)

  test("ServiceDiscoverer.zipWithWeights") {
    val port1 = 80 // not bound
    val port2 = 53 // ditto
    val ents = Seq[Entry](ep(port1), ep(port2), ep(3), ep(4))
    val v1 = Vector(Seq(
      Descriptor(Selector.Host("localhost", port1), 1.1, 1),
      Descriptor(Selector.Host("localhost", port2), 1.4, 1),
      Descriptor(Selector.Member("3"), 3.1, 1)))
    val v2 = Vector(Seq(Descriptor(Selector.Member(port2.toString), 2.0, 1)))
    val vecs = Set(v1, v2)

    assert(ServiceDiscoverer.zipWithWeights(ents, vecs).toSet === Set(
      ep(port1) -> 1.1,
      ep(port2) -> 2.8,
      ep(3) -> 3.1,
      ep(4) -> 1.0))
  }

  test("New observation do not cause reads; entries are cached") {
    implicit val timer = new MockTimer
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val sd = new ServiceDiscoverer(Var.value(new ZkSession(watchedZk)), NullStatsReceiver)

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    val f2 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()

    val ew@ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    val gw@GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(Watched(Node.Children(Seq("member_1"), null), Var.value(WatchState.Pending)))

    assert(!f1.isDefined)
    assert(!f2.isDefined)

    val gd@GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    // ensure that we are hitting the cache: even though we called
    // GetData only once, the two observations are fulfilled.
    assert(f1.isDefined)
    assert(f2.isDefined)
  }
}
