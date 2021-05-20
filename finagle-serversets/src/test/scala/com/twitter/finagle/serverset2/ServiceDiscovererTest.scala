package com.twitter.finagle.serverset2

import com.twitter.finagle.common.io.JsonCodec
import com.twitter.finagle.common.zookeeper.ServerSets
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.serverset2.ServiceDiscoverer.ClientHealth
import com.twitter.finagle.serverset2.ZkOp.{GetData, GetChildrenWatch, ExistsWatch}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import com.twitter.thrift
import com.twitter.thrift.ServiceInstance
import com.twitter.util._
import org.mockito.Mockito.when
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatestplus.mockito.MockitoSugar
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.funsuite.AnyFunSuite

class ServiceDiscovererTest
    extends AnyFunSuite
    with MockitoSugar
    with Eventually
    with IntegrationPatience {

  class ServiceDiscovererWithExposedCache(
    varZkSession: Var[ZkSession],
    statsReceiver: StatsReceiver,
    timer: Timer = DefaultTimer)
      extends ServiceDiscoverer(varZkSession, statsReceiver, ForeverEpoch, timer) {
    val cache = new ZkEntryCache("/foo/bar", NullStatsReceiver)
    cache.setSession(varZkSession.sample)
    override val entriesOf = Memoize { path: String =>
      entitiesOf(path, cache, NullStatsReceiver.stat("meh"), ServiceDiscoverer.EndpointGlob)
    }
  }

  def ep(port: Int) =
    Endpoint(
      Array(null),
      "localhost",
      port,
      Int.MinValue,
      Endpoint.Status.Alive,
      port.toString,
      Map.empty)
  val ForeverEpoch = Epoch(Duration.Top, new MockTimer)
  val retryStream = RetryStream()

  def createEntry(id: Int): Buf = {
    val jsonCodec = JsonCodec.create(classOf[ServiceInstance])
    val serviceInstance = new ServiceInstance()
    serviceInstance.setShard(1)
    serviceInstance.setStatus(thrift.Status.ALIVE)
    serviceInstance.setServiceEndpoint(new thrift.Endpoint(s"$id.0.0.12", 32123))
    ByteArray.Owned(ServerSets.serializeServiceInstance(serviceInstance, jsonCodec))
  }

  test("ServiceDiscoverer.zipWithWeights") {
    val port1 = 80 // not bound
    val port2 = 53 // ditto
    val ents = Seq[Entry](ep(port1), ep(port2), ep(3), ep(4))
    val v1 = Vector(
      Seq(
        Descriptor(Selector.Host("localhost", port1), 1.1, 1),
        Descriptor(Selector.Host("localhost", port2), 1.4, 1),
        Descriptor(Selector.Member("3"), 3.1, 1)
      )
    )
    val v2 = Vector(Seq(Descriptor(Selector.Member(port2.toString), 2.0, 1)))
    val vecs = Seq(v1, v2)

    assert(
      ServiceDiscoverer
        .zipWithWeights(ents, vecs.toSet)
        .toSet == Set(ep(port1) -> 1.1, ep(port2) -> 2.8, ep(3) -> 3.1, ep(4) -> 1.0)
    )
  }

  test("New observation do not cause reads; entries are cached") {
    implicit val timer = new MockTimer
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val sd = new ServiceDiscoverer(
      Var.value(new ZkSession(retryStream, watchedZk, NullStatsReceiver)),
      NullStatsReceiver,
      ForeverEpoch,
      timer
    )

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()

    val ew @ ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    val gw @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(Watched(Node.Children(Seq("member_1"), null), Var.value(WatchState.Pending)))

    assert(!f1.isDefined)

    val gd @ GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    assert(f1.isDefined)

    val f2 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    assert(f2.isDefined)
  }

  test("Removed entries are removed from cache") {
    implicit val timer = new MockTimer
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val sd = new ServiceDiscovererWithExposedCache(
      Var.value(new ZkSession(retryStream, watchedZk, NullStatsReceiver)),
      NullStatsReceiver
    )

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    val cache = sd.cache

    val ew @ ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    assert(cache.keys == Set.empty)

    val gw @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(
      Watched(
        Node.Children(Seq("member_1"), null),
        Var.value(new WatchState.Determined(NodeEvent.Created))
      )
    )

    val gd @ GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    assert(cache.keys == Set("member_1"))

    val gw2 @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(3)
    gw2.res() = Return(
      Watched(
        Node.Children(Seq.empty, null),
        Var.value(new WatchState.Determined(NodeEvent.Created))
      )
    )

    assert(cache.keys == Set.empty)

    val gw3 @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(4)
    gw3.res() = Return(
      Watched(
        Node.Children(Seq("member_2"), null),
        Var.value(new WatchState.Determined(NodeEvent.Created))
      )
    )

    val gd2 @ GetData("/foo/bar/member_2") = watchedZk.value.opq(5)
    gd2.res() = Return(Node.Data(None, null))

    assert(cache.keys == Set("member_2"))

    val gw4 @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(6)
    gw4.res() = Return(
      Watched(
        Node.Children(Seq("member_3", "member_4"), null),
        Var.value(new WatchState.Determined(NodeEvent.Created))
      )
    )

    val gd3 @ GetData("/foo/bar/member_3") = watchedZk.value.opq(7)
    gd3.res() = Return(Node.Data(None, null))
    val gd4 @ GetData("/foo/bar/member_4") = watchedZk.value.opq(8)
    gd4.res() = Return(Node.Data(None, null))

    assert(cache.keys == Set("member_3", "member_4"))
  }

  test("If all reads fail the serverset is in Failed state") {
    implicit val timer = new MockTimer
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val sd = new ServiceDiscovererWithExposedCache(
      Var.value(new ZkSession(retryStream, watchedZk, NullStatsReceiver)),
      NullStatsReceiver,
      timer
    )

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    val cache = sd.cache

    val ew @ ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    val gw @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(
      Watched(
        Node.Children(Seq("member_1", "member_2"), null),
        Var.value(new WatchState.Determined(NodeEvent.Created))
      )
    )

    val gd @ GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Throw(new Exception)

    val gd2 @ GetData("/foo/bar/member_2") = watchedZk.value.opq(3)
    gd2.res() = Throw(new Exception)

    Await.result(f1, 1.second) match {
      case Activity.Failed(ServiceDiscoverer.EntryLookupFailureException) => // great!
      case other => fail(s"Expected entry lookup exception. Received $other")
    }
  }

  test("Partial failures are successful and retried") {
    Time.withCurrentTimeFrozen { timeControl =>
      implicit val timer = new MockTimer
      val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
      val sd = new ServiceDiscovererWithExposedCache(
        Var.value(new ZkSession(retryStream, watchedZk, NullStatsReceiver)),
        NullStatsReceiver,
        timer
      )

      val currentValue = new AtomicReference[Activity.State[Seq[(Entry, Double)]]]

      // Test will become flaky if we don't capture this as the Closeable will be occasionally
      // closed by the CollectCloseables thread
      val holdRef =
        sd("/foo/bar").states.filter(_ != Activity.Pending).register(Witness(currentValue))
      val cache = sd.cache

      val ew @ ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
      val ewwatchv = Var[WatchState](WatchState.Pending)
      ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

      val gw @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
      gw.res() = Return(
        Watched(Node.Children(Seq("member_1", "member_2"), null), Var.value(WatchState.Pending))
      )

      val gd @ GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
      gd.res() = Throw(new Exception)

      val gd2 @ GetData("/foo/bar/member_2") = watchedZk.value.opq(3)
      gd2.res() = Return(Node.Data(Some(createEntry(1)), null))

      // Should succeed with only 1 resolved value
      eventually {
        currentValue.get match {
          case Activity.Ok(seq) => assert(seq.size == 1) // member_2 has good data
          case other => fail(s"Expected entry lookup exception. Received $other")
        }
      }

      // member_1 will be requeried for eventually
      eventually {
        timeControl.advance(2.minutes)
        timer.tick()

        val gd3 @ GetData("/foo/bar/member_1") = watchedZk.value.opq(4)
        gd3.res() = Return(Node.Data(Some(createEntry(2)), null))
      }

      // Then we should see 2 values in the serverset
      currentValue.get match {
        case Activity.Ok(seq) => assert(seq.size == 2) // both have good values now
        case other => fail(s"Expected entry lookup exception. Received $other")
      }
    }
  }

  test("Consecutive observations do not cause reads; entries are cached") {
    implicit val timer = new MockTimer
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val sd = new ServiceDiscoverer(
      Var.value(new ZkSession(retryStream, watchedZk, NullStatsReceiver)),
      NullStatsReceiver,
      ForeverEpoch,
      timer
    )

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    val f2 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()

    val ew @ ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    val gw @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(Watched(Node.Children(Seq("member_1"), null), Var.value(WatchState.Pending)))

    assert(!f1.isDefined)
    assert(!f2.isDefined)

    val gd @ GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    // ensure that we are hitting the cache: even though we called
    // GetData only once, the two observations are fulfilled.
    assert(f1.isDefined)
    assert(f2.isDefined)
  }

  test("New sessions are used") {
    implicit val timer = new MockTimer
    val fakeWatchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val watchedZk = Watched(new OpqueueZkReader(), Var(WatchState.Pending))
    val watchedZkVar =
      new ReadWriteVar(new ZkSession(retryStream, fakeWatchedZk, NullStatsReceiver))
    val sd = new ServiceDiscoverer(watchedZkVar, NullStatsReceiver, ForeverEpoch, timer)

    val f1 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()
    val f2 = sd("/foo/bar").states.filter(_ != Activity.Pending).toFuture()

    watchedZkVar.update(new ZkSession(retryStream, watchedZk, NullStatsReceiver))

    val ew @ ExistsWatch("/foo/bar") = watchedZk.value.opq(0)
    val ewwatchv = Var[WatchState](WatchState.Pending)
    ew.res() = Return(Watched(Some(Data.Stat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)), ewwatchv))

    val gw @ GetChildrenWatch("/foo/bar") = watchedZk.value.opq(1)
    gw.res() = Return(Watched(Node.Children(Seq("member_1"), null), Var.value(WatchState.Pending)))

    assert(!f1.isDefined)
    assert(!f2.isDefined)

    val gd @ GetData("/foo/bar/member_1") = watchedZk.value.opq(2)
    gd.res() = Return(Node.Data(None, null))

    // ensure that we are hitting the cache: even though we called
    // GetData only once, the two observations are fulfilled.
    assert(f1.isDefined)
    assert(f2.isDefined)
  }

  def newZkSession(): (ZkSession, Witness[WatchState]) = {
    val mockZkSession = mock[ZkSession]
    val watchStateEvent = Event[WatchState]()
    val watchStateVar = Var[WatchState](WatchState.Pending, watchStateEvent)
    when(mockZkSession.state).thenReturn(watchStateVar)

    (mockZkSession, watchStateEvent)
  }

  test("ServiceDiscoverer stable health is reported correctly") {
    Time.withCurrentTimeFrozen { timeControl =>
      val zkSession = Event[ZkSession]()
      val varZkSession = Var[ZkSession](ZkSession.nil, zkSession)
      val period = 1.second
      implicit val timer = new MockTimer
      val sd = new ServiceDiscoverer(varZkSession, NullStatsReceiver, Epoch(period, timer), timer)

      val stabilizedHealth = new AtomicReference[ClientHealth](ClientHealth.Healthy)
      sd.health.changes.register(Witness {
        stabilizedHealth
      })

      // should start as unknown until updated otherwise
      assert(stabilizedHealth.get == ClientHealth.Unknown)

      val (session1, state1) = newZkSession()
      state1.notify(WatchState.SessionState(SessionState.SyncConnected))
      zkSession.notify(session1)
      assert(stabilizedHealth.get == ClientHealth.Healthy)

      // make unhealthy without turning the clock
      state1.notify(WatchState.SessionState(SessionState.Expired))
      assert(stabilizedHealth.get == ClientHealth.Healthy)
      timer.tick()

      //advance past the health period to make the stabilized health unhealthy
      timeControl.advance(period)
      timer.tick()
      assert(stabilizedHealth.get == ClientHealth.Unhealthy)

      // flip to a new session
      val (session2, state2) = newZkSession()
      state2.notify(WatchState.SessionState(SessionState.SyncConnected))
      zkSession.notify(session2)
      assert(stabilizedHealth.get == ClientHealth.Healthy)
    }
  }

  test("ServiceDiscoverer rawHealth is reported correctly") {
    val zkSession = Event[ZkSession]()
    val varZkSession = Var[ZkSession](ZkSession.nil, zkSession)
    val sd = new ServiceDiscoverer(varZkSession, NullStatsReceiver, ForeverEpoch, DefaultTimer)

    val health = new AtomicReference[ClientHealth](ClientHealth.Healthy)
    sd.rawHealth.changes.register(Witness {
      health
    })

    // should start as unknown until updated otherwise
    assert(health.get == ClientHealth.Unknown)

    val (session1, state1) = newZkSession()
    state1.notify(WatchState.SessionState(SessionState.SyncConnected))
    zkSession.notify(session1)
    assert(health.get == ClientHealth.Healthy)

    // make unhealthy
    state1.notify(WatchState.SessionState(SessionState.Expired))
    assert(health.get == ClientHealth.Unhealthy)

    // flip to a new session
    val (session2, state2) = newZkSession()
    state2.notify(WatchState.SessionState(SessionState.SyncConnected))
    zkSession.notify(session2)
    assert(health.get == ClientHealth.Healthy)

    // pulse the bad session (which is NOT the current session) and ensure we stay healthy
    state1.notify(WatchState.SessionState(SessionState.Disconnected))
    assert(health.get == ClientHealth.Healthy)

    // pulse the current session with an event that should be ignored
    state2.notify(WatchState.Pending)
    assert(health.get == ClientHealth.Healthy)
  }
}
