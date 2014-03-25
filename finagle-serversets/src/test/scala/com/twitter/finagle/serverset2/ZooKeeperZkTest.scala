package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.serverset2.client._
import com.twitter.finagle.zookeeper.ZkInstance
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration, Promise, Stopwatch, Var}
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually._
import org.scalatest.junit.JUnitRunner
import org.scalatest.time._
import org.scalatest.{BeforeAndAfter, FunSuite, Tag}

@RunWith(classOf[JUnitRunner])
class ZooKeeperZkTest extends FunSuite with BeforeAndAfter {
  val zkTimeout = 100.milliseconds
  @volatile var inst: ZkInstance = _

  def toSpan(d: Duration): Span = Span(d.inNanoseconds, Nanoseconds)

  implicit val patienceConfig = PatienceConfig(
    timeout = toSpan(1.second),
    interval = toSpan(zkTimeout))

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

  // Tagging tests as flaky until https://jira.twitter.biz/browse/COORD-339 is resolved
  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    if (!sys.props.contains("SKIP_FLAKY"))
      super.test(testName, testTags:_*)(f)
  }

  test("Session expiration 2") {
    val connected: (WatchState => Boolean) = {
      case WatchState.SessionState(SessionState.SyncConnected) => true
      case _ => false
    }
    val notConnected: (WatchState => Boolean) = w => !connected(w)

    val zk1 = Zk.retrying(zkTimeout, () => Zk(inst.zookeeperConnectstring))
    @volatile var states = Seq.empty[SessionState]
    val state = zk1 flatMap { zk1 => zk1.state }
    state observe {
      case WatchState.SessionState(s) => states = s +: states
      case _ =>
    }

    Await.result(state.observeUntil(connected))
    val cond = state.observeUntil(notConnected)

    val zk2 = {
      val z = Var.sample(zk1)
      val p = new Array[Byte](z.sessionPasswd.length)
      z.sessionPasswd.write(p, 0)

      ClientBuilder()
        .hosts(inst.zookeeperConnectstring)
        .sessionTimeout(zkTimeout)
        .sessionId(z.sessionId)
        .password(Buf.ByteArray(p))
        .reader()
    }
    Await.result(zk2.state.observeUntil(connected))
    zk2.value.close()

    Await.result(cond)
    Await.result(state.observeUntil(connected))

    assert(states === Seq(
      SessionState.SyncConnected, SessionState.Expired,
      SessionState.Disconnected, SessionState.SyncConnected))
  }

  test("Zk.retrying") {
    val watch = Stopwatch.start()

    val zk = Zk.retrying(zkTimeout, () => Zk(inst.zookeeperConnectstring))

    val zkState = for (zk <- zk; state <- zk.state) yield state
    @volatile var zkStates = Seq[(SessionState, Duration)]()
    zkState observe {
      case WatchState.SessionState(state) =>
        zkStates = (state, watch()) +: zkStates
      case _ =>
    }

    @volatile var zks = Seq[Zk]()
    zk observe { zk => zks = zk +: zks }

    // Wait for the initial connect.
    eventually {
      assert(Var.sample(zkState) ===
        WatchState.SessionState(SessionState.SyncConnected))
      assert(zks.size === 1)
    }

    val zk1 = Var.sample(zk)

    // Hijack the session by reusing its id and password.
    val zk2 = {
      val p = new Array[Byte](zk1.sessionPasswd.length)
      zk1.sessionPasswd.write(p, 0)

      ClientBuilder()
        .hosts(inst.zookeeperConnectstring)
        .sessionTimeout(zkTimeout)
        .sessionId(zk1.sessionId)
        .password(Buf.ByteArray(p))
        .reader()
    }

    val connected = new Promise[Unit]
    val closed = new Promise[Unit]
    zk2.state observe {
      case WatchState.SessionState(SessionState.SyncConnected) =>
        connected.setDone()
      case WatchState.SessionState(SessionState.Disconnected) =>
        closed.setDone()
      case _ =>
    }

    Await.ready(connected)
    Await.ready(zk2.value.close())

    // This will expire the session.
    val zk1Expired = zk1.state.observeUntil(
      _ == WatchState.SessionState(SessionState.Expired))
    val zkConnected = zkState.observeUntil(
      _ == WatchState.SessionState(SessionState.SyncConnected))

    Await.ready(zk1.getData("/sadfads"))
    Await.ready(zk1Expired)
    Await.ready(zkConnected)

    eventually {
      assert((zkStates map { case (s, _) => s }).reverse ===
        Seq(SessionState.SyncConnected, SessionState.Disconnected,
          SessionState.Expired, SessionState.SyncConnected))
    }
    assert(zks.size === 2)
  }

}
