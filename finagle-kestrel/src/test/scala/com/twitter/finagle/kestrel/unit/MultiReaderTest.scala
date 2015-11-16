package com.twitter.finagle.kestrel.unit

import _root_.java.net.{InetSocketAddress, SocketAddress}
import _root_.java.nio.charset.Charset
import _root_.java.util.concurrent.{BlockingDeque, ExecutorService, Executors, LinkedBlockingDeque}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.twitter.concurrent.{Broker, Spool}
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig, Cluster}
import com.twitter.finagle.kestrel._
import com.twitter.finagle.kestrel.protocol.{Command, Response, Set}
import com.twitter.finagle.{Addr, ClientConnection, Service, ServiceFactory}
import com.twitter.io.Buf
import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.collection.immutable.{Set => ISet}
import scala.collection.mutable.{ArrayBuffer, Set => MSet}

@RunWith(classOf[JUnitRunner])
class MultiReaderTest extends FunSuite with MockitoSugar with Eventually with IntegrationPatience {
  class MockHandle extends ReadHandle {
    val _messages = new Broker[ReadMessage]
    val _error = new Broker[Throwable]

    val messages = _messages.recv
    val error = _error.recv
    def close() {} // to spy on!
  }

  trait MultiReaderHelper {
    val queueName = "the_queue"
    val queueNameBuf = Buf.Utf8(queueName)
    val N = 3
    val handles = (0 until N) map { _ => Mockito.spy(new MockHandle) }
    val va: Var[Return[ISet[ReadHandle]]] = Var.value(Return(handles.toSet))
  }

  trait AddrClusterHelper {
    val queueName = "the_queue"
    val queueNameBuf = Buf.Utf8(queueName)
    val N = 3
    val hosts = 0 until N map { i =>
      InetSocketAddress.createUnresolved("10.0.0.%d".format(i), 22133)
    }

    val executor = Executors.newCachedThreadPool()

    def newKestrelService(
      executor: Option[ExecutorService],
      queues: LoadingCache[Buf, BlockingDeque[Buf]]
    ): Service[Command, Response] = {
      val interpreter = new Interpreter(queues)
      new Service[Command, Response] {
        def apply(request: Command) = {
          val promise = new Promise[Response]()
          executor match {
            case Some(exec) =>
              exec.submit(new Runnable {
                def run() {
                  promise.setValue(interpreter(request))
                }
              })
            case None => promise.setValue(interpreter(request))
          }
          promise
        }
      }
    }

    val hostQueuesMap = hosts.map { host =>
      val queues = CacheBuilder.newBuilder()
        .build(new CacheLoader[Buf, BlockingDeque[Buf]] {
        def load(k: Buf) = new LinkedBlockingDeque[Buf]
      })
      (host, queues)
    }.toMap

    lazy val mockClientBuilder = {
      val result = mock[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]]

      hosts.foreach { host =>
        val mockHostClientBuilder =
          mock[ClientBuilder[Command, Response, ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]]
        when(result.hosts(host)) thenReturn mockHostClientBuilder

        val queues = hostQueuesMap(host)
        val factory = new ServiceFactory[Command, Response] {
          // use an executor so readReliably doesn't block waiting on an empty queue
          def apply(conn: ClientConnection) =
            Future.value(newKestrelService(Some(executor), queues))
          def close(deadline: Time) = Future.Done
          override def toString() = "ServiceFactory for %s".format(host)
        }
        when(mockHostClientBuilder.buildFactory()) thenReturn factory
      }
      result
    }

    val services = hosts.map { host =>
      val queues = hostQueuesMap(host)
      // no executor here: this one is used for writing to the queues
      newKestrelService(None, queues)
    }

    def configureMessageReader(handle: ReadHandle): MSet[String] = {
      val messages = MSet[String]()
      val UTF8 = Charset.forName("UTF-8")

      handle.messages foreach { msg =>
        val Buf.Utf8(str) = msg.bytes
        messages += str
        msg.ack.sync()
      }
      messages
    }
  }

  trait DynamicClusterHelper {
    class DynamicCluster[U](initial: Seq[U]) extends Cluster[U] {
      def this() = this(Seq[U]())

      var set = initial.toSet
      var s = new Promise[Spool[Cluster.Change[U]]]

      def add(f: U) = {
        set += f
        performChange(Cluster.Add(f))
      }

      def del(f: U) = {
        set -= f
        performChange(Cluster.Rem(f))
      }

      private[this] def performChange(change: Cluster.Change[U]) = synchronized {
        val newTail = new Promise[Spool[Cluster.Change[U]]]
        s() = Return(change *:: newTail)
        s = newTail
      }

      def snap = (set.toSeq, s)
    }

    val N = 3
    val hosts = 0 until N map { i => InetSocketAddress.createUnresolved("10.0.0.%d".format(i), 22133) }

    val executor = Executors.newCachedThreadPool()

    def newKestrelService(
      executor: Option[ExecutorService],
      queues: LoadingCache[Buf, BlockingDeque[Buf]]
    ): Service[Command, Response] = {
      val interpreter = new Interpreter(queues)
      new Service[Command, Response] {
        def apply(request: Command) = {
          val promise = new Promise[Response]()
          executor match {
            case Some(exec) =>
              exec.submit(new Runnable {
                def run() {
                  promise.setValue(interpreter(request))
                }
              })
            case None => promise.setValue(interpreter(request))
          }
          promise
        }
      }
    }

    val hostQueuesMap = hosts.map { host =>
      val queues = CacheBuilder.newBuilder()
        .build(new CacheLoader[Buf, BlockingDeque[Buf]] {
        def load(k: Buf) = new LinkedBlockingDeque[Buf]
      })
      (host, queues)
    }.toMap

    lazy val mockClientBuilder = {
      val result = mock[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]]

      hosts.foreach { host =>
        val mockHostClientBuilder =
          mock[ClientBuilder[Command, Response, ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]]
        when(result.hosts(host)) thenReturn mockHostClientBuilder

        val queues = hostQueuesMap(host)
        val factory = new ServiceFactory[Command, Response] {
          // use an executor so readReliably doesn't block waiting on an empty queue
          def apply(conn: ClientConnection) = Future(newKestrelService(Some(executor), queues))
          def close(deadline: Time) = Future.Done
          override def toString() = "ServiceFactory for %s".format(host)
        }
        when(mockHostClientBuilder.buildFactory()) thenReturn factory
      }
      result
    }

    val services = hosts.map { host =>
      val queues = hostQueuesMap(host)
      // no executor here: this one is used for writing to the queues
      newKestrelService(None, queues)
    }

    def configureMessageReader(handle: ReadHandle): MSet[String] = {
      val messages = MSet[String]()

      handle.messages foreach { msg =>
        val Buf.Utf8(str) = msg.bytes
        messages += str
        msg.ack.sync()
      }
      messages
    }
  }

  test("static ReadHandle cluster should always grab the first available message") {
    new MultiReaderHelper {
      val handle = MultiReaderHelper.merge(va)

      val messages = new ArrayBuffer[ReadMessage]
      handle.messages foreach { messages += _ }

      // stripe some messages across
      val sentMessages = 0 until N * 100 map { _ => mock[ReadMessage] }

      assert(messages.size == 0)
      sentMessages.zipWithIndex foreach { case (m, i) =>
        handles(i % handles.size)._messages ! m
      }

      assert(messages == sentMessages)
    }
  }

  test("static ReadHandle cluster should round robin from multiple available queues") {
    // We use frozen time for deterministic randomness.
    new MultiReaderHelper {
      Time.withTimeAt(Time.epoch + 1.seconds) { _ =>
        // stuff the queues beforehand
        val ms = handles map { h =>
          val m = mock[ReadMessage]
          h._messages ! m
          m
        }

        val handle = MultiReaderHelper.merge(va)
        assert(
          ISet((handle.messages ??), (handle.messages ??), (handle.messages ??)) ==
          ISet(ms(0), ms(1), ms(2))
        )
      }
    }
  }

  test("static ReadHandle cluster should propagate closes") {
    new MultiReaderHelper {
      handles foreach { h => verify(h, times(0)).close() }
      val handle = MultiReaderHelper.merge(va)
      handle.close()
      handles foreach { h => verify(h).close() }
    }
  }

  test("static ReadHandle cluster should propagate errors when everything's errored out") {
    new MultiReaderHelper {
      val handle = MultiReaderHelper.merge(va)
      val e = handle.error.sync()
      handles foreach { h =>
        assert(e.isDefined == false)
        h._error ! new Exception("sad panda")
      }

      assert(e.isDefined == true)
      assert(Await.result(e) == AllHandlesDiedException)
    }
  }

  test("Var[Addr]-based cluster should read messages from a ready cluster") {
    new AddrClusterHelper {
      val va = Var(Addr.Bound(hosts: _*))
      val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(queueNameBuf, Time.now, Buf.Utf8(m))))
      }

      eventually {
        assert(messages == sentMessages.toSet)
      }
    }
  }

  test("Var[Addr]-based cluster should read messages as cluster hosts are added") {
    new AddrClusterHelper {
      val va = Var(Addr.Bound(hosts.head))
      val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(queueNameBuf, Time.now, Buf.Utf8(m))))
      }

      // 0, 3, 6 ...
      eventually {
        assert(messages == sentMessages.grouped(N).map { _.head }.toSet)
      }
      messages.clear()

      va.update(Addr.Bound(hosts: _*))

      // 1, 2, 4, 5, ...
      eventually {
        assert(messages == sentMessages.grouped(N).map { _.tail }.flatten.toSet)
      }
    }
  }

  test("Var[Addr]-based cluster should read messages as cluster hosts are removed") {
    new AddrClusterHelper {
      var mutableHosts: Seq[SocketAddress] = hosts
      val va = Var(Addr.Bound(mutableHosts: _*))
      val rest = hosts.tail.reverse
      val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()

      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(queueNameBuf, Time.now, Buf.Utf8(m))))
      }

      eventually {
        assert(messages == sentMessages.toSet)
      }
      rest.zipWithIndex.foreach { case (host, hostIndex) =>
        messages.clear()
        mutableHosts = (mutableHosts.toSet - host).toSeq
        va.update(Addr.Bound(mutableHosts: _*))

        // write to all 3
        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set(queueNameBuf, Time.now, Buf.Utf8(m))))
        }

        // expect fewer to be read on each pass
        val expectFirstN = N - hostIndex - 1
        eventually {
          assert(messages == sentMessages.grouped(N).map { _.take(expectFirstN) }.flatten.toSet)
        }
      }
    }
  }

  test("Var[Addr]-based cluster should wait for cluster to become ready before snapping initial hosts") {
    new AddrClusterHelper {
      val va = Var(Addr.Bound())
      val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
      val messages = configureMessageReader(handle)
      val error = handle.error.sync()
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(queueNameBuf, Time.now, Buf.Utf8(m))))
      }

      assert(messages.size == 0) // cluster not ready
      assert(error.isDefined == false)

      va.update(Addr.Bound(hosts: _*))

      eventually {
        assert(messages == sentMessages.toSet)
      }
    }
  }

  test("Var[Addr]-based cluster should report an error if all hosts are removed") {
    new AddrClusterHelper {
      val va = Var(Addr.Bound(hosts: _*))
      val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
      val error = handle.error.sync()
      va.update(Addr.Bound())

      assert(error.isDefined == true)
      assert(Await.result(error) == AllHandlesDiedException)
    }
  }

  test("Var[Addr]-based cluster should propagate exception if cluster fails") {
    new AddrClusterHelper {
      val ex = new Exception("uh oh")
      val va: Var[Addr] with Updatable[Addr] = Var(Addr.Bound(hosts: _*))
      val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
      val error = handle.error.sync()
      va.update(Addr.Failed(ex))

      assert(error.isDefined == true)
      assert(Await.result(error) == ex)
    }
  }

  test("dynamic SocketAddress cluster should read messages from a ready cluster") {
    new DynamicClusterHelper {
      val cluster = new DynamicCluster[SocketAddress](hosts)
      val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
      }

      eventually {
        assert(messages == sentMessages.toSet)
      }
    }
  }

  test("dynamic SocketAddress cluster should read messages as cluster hosts are added") {
    new DynamicClusterHelper {
      val (host, rest) = (hosts.head, hosts.tail)
      val cluster = new DynamicCluster[SocketAddress](List(host))
      val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
      }

      // 0, 3, 6 ...
      eventually {
        assert(messages == sentMessages.grouped(N).map { _.head }.toSet)
      }
      messages.clear()

      rest.foreach { host => cluster.add(host) }

      // 1, 2, 4, 5, ...
      eventually {
        assert(messages == sentMessages.grouped(N).map { _.tail }.flatten.toSet)
      }
    }
  }

  test("dynamic SocketAddress cluster should read messages as cluster hosts are removed") {
    new DynamicClusterHelper {
      val cluster = new DynamicCluster[SocketAddress](hosts)
      val rest = hosts.tail
      val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()

      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
      }

      eventually {
        assert(messages == sentMessages.toSet)
      }

      rest.reverse.zipWithIndex.foreach { case (host, hostIndex) =>
        messages.clear()
        cluster.del(host)

        // write to all 3
        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
        }

        // expect fewer to be read on each pass
        val expectFirstN = N - hostIndex - 1
        eventually {
          assert(messages == sentMessages.grouped(N).map { _.take(expectFirstN) }.flatten.toSet)
        }
      }
    }
  }

  test("dynamic SocketAddress cluster should wait " +
    "for cluster to become ready before snapping initial hosts") {
    new DynamicClusterHelper {
      val cluster = new DynamicCluster[SocketAddress](Seq())
      val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
      val messages = configureMessageReader(handle)
      val errors = (handle.error ?)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }
      assert(messages.size == 0)

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
      }

      assert(messages.size == 0) // cluster not ready
      assert(errors.isDefined == false)

      hosts.foreach { host => cluster.add(host) }

      eventually {
        assert(messages == sentMessages.toSet)
      }
    }
  }

  test("dynamic SocketAddress cluster should report an error if all hosts are removed") {
    new DynamicClusterHelper {
      val cluster = new DynamicCluster[SocketAddress](hosts)
      val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
      val e = (handle.error ?)
      hosts.foreach { host => cluster.del(host) }

      assert(e.isDefined == true)
      assert(Await.result(e) == AllHandlesDiedException)
    }
  }

  test("dynamic SocketAddress cluster should silently" +
    " handle the removal of a host that was never added") {
    new DynamicClusterHelper {
      val cluster = new DynamicCluster[SocketAddress](hosts)
      val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()

      val messages = configureMessageReader(handle)
      val sentMessages = 0 until N * 10 map { i => "message %d".format(i) }

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
      }
      eventually {
        assert(messages == sentMessages.toSet)
      }
      messages.clear()

      cluster.del(InetSocketAddress.createUnresolved("10.0.0.100", 22133))

      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(Buf.Utf8("the_queue"), Time.now, Buf.Utf8(m))))
      }
      eventually {
        assert(messages == sentMessages.toSet)
      }
    }
  }
}
