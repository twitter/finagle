package com.twitter.finagle.integration

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle._
import com.twitter.finagle.exp.pushsession.{MockChannelHandle, PipeliningClientPushSession, PushChannelHandle}
import com.twitter.finagle.http
import com.twitter.finagle.memcached.{protocol => memcached}
import com.twitter.finagle.http.codec.HttpClientDispatcher
import com.twitter.finagle.http.exp.IdentityStreamTransport
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.{QueueTransport, Transport}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import com.twitter.util.TimeConversions._
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

/**
 * We want client session statuses to reflect the status of their underlying transports/handles
 */
class ClientSessionTest extends FunSuite with MockitoSugar {

  def testSessionStatus[Req, Rep](
    name: String,
    sessionFac: (Transport[Req, Rep]) => () => Status
  ): Unit = {

    test(s"$name: session status reflects underlying transport") {
      val clientToServer = new AsyncQueue[Req]
      val serverToClient = new AsyncQueue[Rep]

      val transport: Transport[Req, Rep] =
        new QueueTransport(writeq = clientToServer, readq = serverToClient)

      val sessionStatus = sessionFac(transport)
      assert(sessionStatus() == Status.Open)

      Await.ready(transport.close(), 5.seconds)
      assert(sessionStatus() == Status.Closed)
    }
  }

  def testPushSessionStatus[In, Out](
    name: String,
    pushSessionFac: (PushChannelHandle[In, Out]) => () => Status
  ): Unit = {

    test(s"$name: session status reflects underlying handle") {
      val handle = new MockChannelHandle[In, Out]

      val sessionStatus = pushSessionFac(handle)
      assert(sessionStatus() == Status.Open)

      handle.status = Status.Closed
      assert(sessionStatus() == Status.Closed)
    }
  }

  testSessionStatus[mux.transport.Message, mux.transport.Message](
    "mux-transport", { tr: Transport[mux.transport.Message, mux.transport.Message] =>
      val session: mux.ClientSession =
        new mux.ClientSession(tr, liveness.FailureDetector.NullConfig, "test", NullStatsReceiver)
      () =>
        session.status
    }
  )

  testSessionStatus[mux.transport.Message, mux.transport.Message](
    "mux-dispatcher", { tr: Transport[mux.transport.Message, mux.transport.Message] =>
      val dispatcher = mux.ClientDispatcher.newRequestResponse(tr)
      () =>
        dispatcher.status
    }
  )

  testSessionStatus(
    "http-transport", { tr: Transport[Any, Any] =>
      val manager = mock[http.codec.ConnectionManager]
      val closeP = new Promise[Unit]
      when(manager.shouldClose).thenReturn(false)
      when(manager.onClose).thenReturn(closeP)
      val wrappedT = new http.HttpTransport(
        new IdentityStreamTransport(Transport.cast[http.Request, http.Response](tr)),
        manager
      )
      () =>
        wrappedT.status
    }
  )

  testSessionStatus(
    "http-dispatcher", { tr: Transport[Any, Any] =>
      val dispatcher = new HttpClientDispatcher(
        new IdentityStreamTransport(Transport.cast[http.Request, http.Response](tr)),
        NullStatsReceiver
      )
      () =>
        dispatcher.status
    }
  )

  class MyPushClient extends com.twitter.finagle.Memcached.Client.PushClient {
    def toSvc(
      handle: PushChannelHandle[memcached.Response, memcached.Command]
    ): Service[memcached.Command, memcached.Response] = {
      val session = new PipeliningClientPushSession[memcached.Response, memcached.Command](
        handle,
        NullStatsReceiver,
        Duration.Top,
        DefaultTimer
      )
      Await.result(toService(session), 5.seconds)
    }
  }

  testPushSessionStatus(
    "memcached-session", { handle: PushChannelHandle[memcached.Response, memcached.Command] =>
      val cl: MyPushClient = new MyPushClient
      val svc = cl.toSvc(handle)
      () =>
        svc.status
    }
  )

  class MyNonPushClient extends com.twitter.finagle.Memcached.Client.NonPushClient {
    def newDisp(
      transport: Transport[memcached.Command, memcached.Response]
    ): Service[memcached.Command, memcached.Response] =
      super.newDispatcher(transport)
  }

  testSessionStatus(
    "memcached-dispatcher", { tr: Transport[memcached.Command, memcached.Response] =>
      val cl: MyNonPushClient = new MyNonPushClient
      val svc = cl.newDisp(tr)
      () =>
        svc.status
    }
  )

  testSessionStatus(
    "mysql-dispatcher", { tr: Transport[mysql.transport.Packet, mysql.transport.Packet] =>
      val handshake = mysql.Handshake(Some("username"), Some("password"))
      val dispatcher = new mysql.ClientDispatcher(tr, handshake, false)
      () =>
        dispatcher.status
    }
  )
}
