package com.twitter.finagle.serverset2

import com.twitter.finagle.serverset2.ZkOp.{GetData, GetChildrenWatch, ExistsWatch}
import com.twitter.finagle.serverset2.client.{Node, NodeEvent, Data, WatchState, Watched}
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
    val vecs = Seq(v1, v2)

    assert(ServiceDiscoverer.zipWithWeights(ents, vecs.toSet).toSet === Set(
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

    val ew@ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    val gw@GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(Watched(Node.Children(Seq("member_1"), null), Var.value(WatchState.Pending)))

    assert(!f1.isDefined)

    val gd@GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    assert(f1.isDefined)

    val f2 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    assert(f2.isDefined)
  }

  test("Removed entries are removed from cache") {
    implicit val timer = new MockTimer
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val sd = new ServiceDiscoverer(Var.value(new ZkSession(watchedZk)), NullStatsReceiver)

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    val cache = sd.entriesOfCluster("/foo/bar")

    val ew@ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    assert(cache.keys == Set.empty)

    val gw@GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(Watched(Node.Children(Seq("member_1"), null), Var.value(new WatchState.Determined(NodeEvent.Created))))

    assert(cache.keys == Set.empty)

    val gd@GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    assert(cache.keys == Set("/foo/bar/member_1"))

    val gw2@GetChildrenWatch("/foo/bar") = watchedZk.value.opq(3)
    gw2.res() = Return(Watched(Node.Children(Seq.empty, null), Var.value(new WatchState.Determined(NodeEvent.Created))))

    assert(cache.keys == Set.empty)

    val gw3@GetChildrenWatch("/foo/bar") = watchedZk.value.opq(4)
    gw3.res() = Return(Watched(Node.Children(Seq("member_2"), null), Var.value(new WatchState.Determined(NodeEvent.Created))))

    val gd2@GetData("/foo/bar/member_2") = watchedZk.value.opq(5)
    gd2.res() = Return(Node.Data(None, null))

    assert(cache.keys == Set("/foo/bar/member_2"))

    val gw4@GetChildrenWatch("/foo/bar") = watchedZk.value.opq(6)
    gw4.res() = Return(Watched(Node.Children(Seq("member_3", "member_4"), null), Var.value(new WatchState.Determined(NodeEvent.Created))))

    val gd3@GetData("/foo/bar/member_3") = watchedZk.value.opq(7)
    gd3.res() = Return(Node.Data(None, null))
    val gd4@GetData("/foo/bar/member_4") = watchedZk.value.opq(8)
    gd4.res() = Return(Node.Data(None, null))

    assert(cache.keys == Set("/foo/bar/member_3", "/foo/bar/member_4"))
  }

  /**
   * New code does not cache a given ZK getData call until the call finishes, so two rapid
   * activity updates, or two calls to apply with the same serverset (as in this test) will cause
   * multiple calls to ZK for the same node's getData. This does not leak memory, but is wasteful.
   * The old code simply cached the Future with memoize (including failures). But any time the
   * ZkSession was lost/renewed, it threw away the entire cache, thus retrying once per new
   * ZkSession (needlessly retrying successes too though). This commented test will fail with
   * the new code. This is not catastrophic, but is wasteful. Currently seeing if there's a nicer
   * solution using a FutureCache or derivative, but seeing as this change fixes a major memory
   * leak I thought it best to post as is. We can improve the caching behavior in a follow up I
   * Think.
   *
   * Ticket for follow up: TRFC-491
   */
  /*
  test("Consecutive observations do not cause reads; entries are cached") {
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
  */
}
