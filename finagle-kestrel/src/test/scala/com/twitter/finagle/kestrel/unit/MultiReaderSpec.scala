package com.twitter.finagle.kestrel.unit

import _root_.java.net.{InetSocketAddress, SocketAddress}
import _root_.java.nio.charset.Charset
import _root_.java.util.concurrent._
import com.google.common.cache.{LoadingCache, CacheLoader, CacheBuilder}
import com.twitter.concurrent.{Broker, Spool}
import com.twitter.conversions.time._
import com.twitter.finagle.{Addr, ClientConnection, Service, ServiceFactory}
import com.twitter.finagle.builder.{ClientConfig, ClientBuilder, Cluster}
import com.twitter.finagle.kestrel._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Await, Future, Promise, Return, Time, Updatable, Var}
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.mutable.{ArrayBuffer, Set => MSet}
import scala.collection.immutable.{Set => ISet}

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Suites}
import org.scalatest.matchers._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{verify, times, when}

trait CollectionComparison {
  def sameAs[A](c: Traversable[A], d: Traversable[A]): Boolean = 
    if (c.isEmpty) d.isEmpty
    else {
      val (e, f) = d span (c.head !=)
      if (f.isEmpty) false else sameAs(c.tail, e ++ f.tail)
    }
}

@RunWith(classOf[JUnitRunner])
class MultiReaderTest extends Suites(
  new StaticReadHandleClusterTest,
  new VarAddrBasedClusterTest,
  new DynamicSocketAddressClusterTest
) 

class MockHandle extends ReadHandle {
  val _messages = new Broker[ReadMessage]
  val _error = new Broker[Throwable]

  val messages = _messages.recv
  val error = _error.recv
  def close() {} // to spy on!
}

class StaticReadHandleClusterTest extends FunSuite with MockitoSugar {
  val N = 3
  val queueName = "the_queue"
  val handles = (0 until N) map { _ => mock[MockHandle] }
  val va: Var[Return[ISet[ReadHandle]]] = Var.value(Return(handles.toSet))

  test("always grab the first available message") {
    val handle = MultiReaderHelper.merge(va)

    val messages = new ArrayBuffer[ReadMessage]
    handle.messages foreach { messages += _ }

    // stripe some messages across
    val sentMessages = 0 until N*100 map { _ => mock[ReadMessage] }

    assert(messages === Seq.empty)
    sentMessages.zipWithIndex foreach { case (m, i) =>
      handles(i % handles.size)._messages ! m
    }

    assert(messages === sentMessages)
  }

  test("propagate closes") {
    handles foreach { h => verify(h, times(0)).close() }
    val handle = MultiReaderHelper.merge(va)
    handle.close()
    handles foreach { h => verify(h).close() }
  }

  test("propagate errors when everything's errored out") {
    val handle = MultiReaderHelper.merge(va)
    val e = handle.error.sync()
    handles foreach { h =>
      assert(!e.isDefined)
      h._error ! new Exception("sad panda")
    }

    assert(e.isDefined)
    intercept[AllHandlesDiedException.type] {
      Await.result(e)
    }
  }
}

// TODO - this test stopped working
//      // We use frozen time for deterministic randomness.
//      // The message output order was simply determined empirically.
//
//      if (!sys.props.contains("SKIP_FLAKY")) {
//        "round robin from multiple available queues" in Time.withTimeAt(Time.epoch + 1.seconds) { _ =>
//          // stuff the queues beforehand
//          val ms = handles map { h =>
//            val m = mock[ReadMessage]
//            h._messages ! m
//            m
//          }
//
//          val handle = MultiReader.merge(cluster)
//          (handle.messages??) must be_==(ms(0))
//          (handle.messages??) must be_==(ms(2))
//          (handle.messages??) must be_==(ms(1))
//        }
//      }

class VarAddrBasedClusterTest extends FunSuite with MockitoSugar with ShouldMatchers {
  val N = 3
  val queueName = "the_queue"
  val hosts = 0 until N map { i =>
    InetSocketAddress.createUnresolved("10.0.0.%d".format(i), 22133)
  }

  val executor = Executors.newCachedThreadPool()

  def newKestrelService(executor: Option[ExecutorService],
                        queues: LoadingCache[ChannelBuffer, BlockingDeque[ChannelBuffer]]): Service[Command, Response] = {
    val interpreter = new Interpreter(queues)
    new Service[Command, Response] {
      def apply(request: Command) = {
        val promise = new Promise[Response]()
        executor match {
          case Some(executor) =>
            executor.submit(new Runnable {
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
      .build(new CacheLoader[ChannelBuffer, BlockingDeque[ChannelBuffer]] {
      def load(k: ChannelBuffer) = new LinkedBlockingDeque[ChannelBuffer]
    })
    (host, queues)
  }.toMap

  lazy val mockClientBuilder = {
    val result = mock[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]]

    hosts.foreach { host =>
      val mockHostClientBuilder =
        mock[ClientBuilder[Command, Response, ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]]
      when(result.hosts(host)).thenReturn(mockHostClientBuilder)

      val queues = hostQueuesMap(host)
      val factory = new ServiceFactory[Command, Response] {
        // use an executor so readReliably doesn't block waiting on an empty queue
        def apply(conn: ClientConnection) =
          Future.value(newKestrelService(Some(executor), queues))
        def close(deadline: Time) = Future.Done
        override def toString = "ServiceFactory for %s".format(host)
      }
      when(mockHostClientBuilder.buildFactory()).thenReturn(factory)
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
      val str = msg.bytes.toString(UTF8)
      messages += str
      msg.ack.sync()
    }
    messages
  }

  test("read messages from a ready cluster") {
    val va = Var(Addr.Bound(hosts: _*))
    val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages.toSeq === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
    }

    assert(messages.toList === sentMessages.toList)
  }

  test("read messages as cluster hosts are added") {
    val va = Var(Addr.Bound(hosts.head))
    val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages.toSeq === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
    }

    // 0, 3, 6 ...
    assert(messages === sentMessages.grouped(N).map { _.head }.toSet)
    messages.clear()

    va.update(Addr.Bound(hosts: _*))

    // 1, 2, 4, 5, ...
    assert(messages === sentMessages.grouped(N).map { _.tail }.flatten.toSet)
  }

  test("read messages as cluster hosts are removed") {
    var mutableHosts: Seq[SocketAddress] = hosts
    val va = Var(Addr.Bound(mutableHosts: _*))
    val rest = hosts.tail.reverse
    val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()

    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
    }

    assert(messages === sentMessages.toSet)
    rest.zipWithIndex.foreach { case (host, hostIndex) =>
      messages.clear()
      mutableHosts = (mutableHosts.toSet - host).toSeq
      va.update(Addr.Bound(mutableHosts: _*))

      // write to all 3
      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
      }

      // expect fewer to be read on each pass
      val expectFirstN = N - hostIndex - 1
      assert(messages === sentMessages.grouped(N).map { _.take(expectFirstN) }.flatten.toSet)
    }
  }

  test("wait for cluster to become ready before snapping initial hosts") {
    val va = Var(Addr.Bound())
    val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
    val messages = configureMessageReader(handle)
    val error = handle.error.sync()
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
    }

    assert(messages === Seq.empty) // cluster not ready
    assert(!error.isDefined)

    va.update(Addr.Bound(hosts: _*))

    assert(messages === sentMessages.toSet)
  }

  test("report an error if all hosts are removed") {
    val va = Var(Addr.Bound(hosts: _*))
    val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
    val error = handle.error.sync()
    va.update(Addr.Bound())

    assert(error.isDefined)
    intercept[AllHandlesDiedException.type] {
      Await.result(error)
    }        
  }

  test("propagate exception if cluster fails") {
    var ex = new Exception("uh oh")
    val va: Var[Addr] with Updatable[Addr] = Var(Addr.Bound(hosts: _*))
    val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
    val error = handle.error.sync()
    va.update(Addr.Failed(ex))

    assert(error.isDefined)
    intercept[Exception] {
      Await.result(error)
    }
  }
}

class DynamicSocketAddressClusterTest extends FunSuite with MockitoSugar {
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

    private[this] def performChange (change: Cluster.Change[U]) = synchronized {
      val newTail = new Promise[Spool[Cluster.Change[U]]]
      s() = Return(change *:: newTail)
      s = newTail
    }

    def snap = (set.toSeq, s)
  }

  val N = 3
  val hosts = 0 until N map { i => InetSocketAddress.createUnresolved("10.0.0.%d".format(i), 22133) }

  val executor = Executors.newCachedThreadPool()

  def newKestrelService(executor: Option[ExecutorService],
                        queues: LoadingCache[ChannelBuffer, BlockingDeque[ChannelBuffer]]): Service[Command, Response] = {
    val interpreter = new Interpreter(queues)
    new Service[Command, Response] {
      def apply(request: Command) = {
        val promise = new Promise[Response]()
        executor match {
          case Some(executor) =>
            executor.submit(new Runnable {
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
      .build(new CacheLoader[ChannelBuffer, BlockingDeque[ChannelBuffer]] {
      def load(k: ChannelBuffer) = new LinkedBlockingDeque[ChannelBuffer]
    })
    (host, queues)
  }.toMap

  lazy val mockClientBuilder = {
    val result = mock[ClientBuilder[Command, Response, Nothing, ClientConfig.Yes, ClientConfig.Yes]]

    hosts.foreach { host =>
      val mockHostClientBuilder =
        mock[ClientBuilder[Command, Response, ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]]
      when(result.hosts(host)).thenReturn(mockHostClientBuilder)

      val queues = hostQueuesMap(host)
      val factory = new ServiceFactory[Command, Response] {
        // use an executor so readReliably doesn't block waiting on an empty queue
        def apply(conn: ClientConnection) = Future(newKestrelService(Some(executor), queues))
        def close(deadline: Time) = Future.Done
        override def toString = "ServiceFactory for %s".format(host)
      }
      when(mockHostClientBuilder.buildFactory()).thenReturn(factory)
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
      val str = msg.bytes.toString(UTF8)
      messages += str
      msg.ack.sync()
    }
    messages
  }

  test("read messages from a ready cluster") {
    val cluster = new DynamicCluster[SocketAddress](hosts)
    val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
    }

    assert(messages === sentMessages.toSet)
  }

  test("read messages as cluster hosts are added") {
    val (host, rest) = (hosts.head, hosts.tail)
    val cluster = new DynamicCluster[SocketAddress](List(host))
    val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
    }

    // 0, 3, 6 ...
    assert(messages === sentMessages.grouped(N).map { _.head }.toSet)
    messages.clear()

    rest.foreach { host => cluster.add(host) }

    // 1, 2, 4, 5, ...
    assert(messages === sentMessages.grouped(N).map { _.tail }.flatten.toSet)
  }

  test("read messages as cluster hosts are removed") {
    val cluster = new DynamicCluster[SocketAddress](hosts)
    val rest = hosts.tail
    val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()

    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
    }

    assert(messages === sentMessages.toSet)

    rest.reverse.zipWithIndex.foreach { case (host, hostIndex) =>
      messages.clear()
      cluster.del(host)

      // write to all 3
      sentMessages.zipWithIndex foreach { case (m, i) =>
        Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
      }

      // expect fewer to be read on each pass
      val expectFirstN = N - hostIndex - 1
      assert(messages === sentMessages.grouped(N).map { _.take(expectFirstN) }.flatten.toSet)
    }
  }

  test("wait for cluster to become ready before snapping initial hosts") {
    val cluster = new DynamicCluster[SocketAddress](Seq())
    val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
    val messages = configureMessageReader(handle)
    val errors = handle.error?
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
    assert(messages === Seq.empty)

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
    }

    assert(messages === Seq.empty) // cluster not ready
    assert(!errors.isDefined)

    hosts.foreach { host => cluster.add(host) }

    assert(messages === sentMessages.toSet)
  }

  test("report an error if all hosts are removed") {
    val cluster = new DynamicCluster[SocketAddress](hosts)
    val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
    val e = (handle.error?)
    hosts.foreach { host => cluster.del(host) }

    assert(e.isDefined)
    intercept[AllHandlesDiedException.type] {
      Await.result(e)
    }
  }

  test("silently handle the removal of a host that was never added") {
    val cluster = new DynamicCluster[SocketAddress](hosts)
    val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()

    val messages = configureMessageReader(handle)
    val sentMessages = 0 until N*10 map { i => "message %d".format(i) }

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
    }
    assert(messages === sentMessages.toSet)
    messages.clear()

    cluster.del(InetSocketAddress.createUnresolved("10.0.0.100", 22133))

    sentMessages.zipWithIndex foreach { case (m, i) =>
      Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
    }

    assert(messages === sentMessages.toSet)
  }
}