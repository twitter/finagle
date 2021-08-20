package com.twitter.finagle.serverset2

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Backoff
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.io.Buf
import com.twitter.util._
import org.scalatest.concurrent.Eventually
import org.scalatest.time._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class ZkSessionEndToEndTest extends AnyFunSuite with BeforeAndAfter with Eventually {
  val zkTimeout: Duration = 100.milliseconds

  // RetryStream doesn't work with MockTimer (yes, even after we tick it).
  // So, use zero back off to make test deterministic.
  // Alternatively, use a real timer such as ScheduledThreadPoolTimer,
  // which also works but presumably less deterministic.
  //
  // TODO: figure out why MockTimer doesn't work.
  val retryStream = new RetryStream(Backoff.const(0.milliseconds))

  @volatile var inst: ZkInstance = _

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = toSpan(1.second), interval = toSpan(zkTimeout))

  /* This can be useful if you want to retain ZK logging output for debugging.
  val app = new org.apache.log4j.ConsoleAppender
  app.setTarget(org.apache.log4j.ConsoleAppender.SYSTEM_ERR)
  app.setLayout(new org.apache.log4j.SimpleLayout)
  app.activateOptions()
  org.apache.log4j.Logger.getRootLogger().addAppender(app)
   */

  before {
    inst = new ZkInstance
    inst.start()
  }

  after {
    inst.stop()
  }

  test("Session expiration 2") {
    implicit val timer: MockTimer = new MockTimer
    val connected: WatchState => Boolean = {
      case WatchState.SessionState(SessionState.SyncConnected) => true
      case _ => false
    }
    val notConnected: WatchState => Boolean = w => !connected(w)
    val session1 = ZkSession.retrying(
      retryStream,
      () => ZkSession(retryStream, inst.zookeeperConnectString, statsReceiver = NullStatsReceiver)
    )

    @volatile var states = Seq.empty[SessionState]
    val state = session1 flatMap { session1 => session1.state }
    state.changes.register(Witness({ ws: WatchState =>
      ws match {
        case WatchState.SessionState(s) => states = s +: states
        case _ =>
      }
    }))

    Await.result(state.changes.filter(connected).toFuture())
    val cond = state.changes.filter(notConnected).toFuture()

    val session2 = {
      val z = Var.sample(session1)
      val p = new Array[Byte](z.sessionPasswd.length)
      z.sessionPasswd.write(p, 0)

      ClientBuilder()
        .hosts(inst.zookeeperConnectString)
        .sessionTimeout(zkTimeout)
        .sessionId(z.sessionId)
        .password(Buf.ByteArray.Owned(p))
        .reader()
    }
    Await.result(session2.state.changes.filter(connected).toFuture())
    session2.value.close()

    Await.result(cond)
    Await.result(state.changes.filter(connected).toFuture())

    assert(
      states == Seq(
        SessionState.SyncConnected,
        SessionState.Expired,
        SessionState.Disconnected,
        SessionState.SyncConnected
      )
    )
  }

  test("ZkSession.retrying") {
    implicit val timer: MockTimer = new MockTimer
    val watch = Stopwatch.start()
    val varZkSession = ZkSession.retrying(
      retryStream,
      () => ZkSession(retryStream, inst.zookeeperConnectString, statsReceiver = NullStatsReceiver)
    )
    val varZkState = varZkSession flatMap { _.state }

    @volatile var zkStates = Seq[(SessionState, Duration)]()
    varZkState.changes.register(Witness({ ws: WatchState =>
      ws match {
        case WatchState.SessionState(state) =>
          zkStates = (state, watch()) +: zkStates
        case _ =>
      }
    }))

    @volatile var sessions = Seq[ZkSession]()
    // The initial size of sessions is 1, because the current value
    // of varZkSession (WatchState.pending) is emitted upon subscription.
    varZkSession.changes.register(Witness({ s: ZkSession => sessions = s +: sessions }))

    // Wait for the initial connect.
    eventually {
      assert(
        Var.sample(varZkState) ==
          WatchState.SessionState(SessionState.SyncConnected)
      )
      assert(sessions.size == 2)
    }

    val session1 = Var.sample(varZkSession)

    // Hijack the session by reusing its id and password.
    val session2 = {
      val p = new Array[Byte](session1.sessionPasswd.length)
      session1.sessionPasswd.write(p, 0)

      ClientBuilder()
        .hosts(inst.zookeeperConnectString)
        .sessionTimeout(zkTimeout)
        .sessionId(session1.sessionId)
        .password(Buf.ByteArray.Owned(p))
        .reader()
    }

    val connected = new Promise[Unit]
    val closed = new Promise[Unit]
    session2.state.changes.register(Witness({ ws: WatchState =>
      ws match {
        case WatchState.SessionState(SessionState.SyncConnected) =>
          connected.setDone(); ()
        case WatchState.SessionState(SessionState.Disconnected) =>
          closed.setDone(); ()
        case _ => ()
      }
    }))

    Await.ready(connected)
    Await.ready(session2.value.close())

    // This will expire the session.
    val session1Expired =
      session1.state.changes.filter(_ == WatchState.SessionState(SessionState.Expired)).toFuture()
    val zkConnected =
      varZkState.changes.filter(_ == WatchState.SessionState(SessionState.SyncConnected)).toFuture()

    Await.ready(session1.getData("/sadfads"))
    Await.ready(session1Expired)
    Await.ready(zkConnected)

    eventually {
      assert(
        (zkStates map { case (s, _) => s }).reverse ==
          Seq(
            SessionState.SyncConnected,
            SessionState.Disconnected,
            SessionState.Expired,
            SessionState.SyncConnected
          )
      )
    }
    assert(sessions.size == 3)
  }
}
