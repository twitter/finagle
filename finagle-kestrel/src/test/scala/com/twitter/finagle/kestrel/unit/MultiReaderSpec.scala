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
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import scala.collection.mutable.{ArrayBuffer, Set => MSet}
import scala.collection.immutable.{Set => ISet}

class MultiReaderSpec extends SpecificationWithJUnit with Mockito {
  noDetailedDiffs()

  class MockHandle extends ReadHandle {
    val _messages = new Broker[ReadMessage]
    val _error = new Broker[Throwable]

    val messages = _messages.recv
    val error = _error.recv
    def close() {} // to spy on!
  }

  "MultiReader" should {
    val queueName = "the_queue"
    "static ReadHandle cluster" in {
      val N = 3
      val handles = (0 until N) map { _ => spy(new MockHandle) }
      val va: Var[Return[ISet[ReadHandle]]] = Var.value(Return(handles.toSet))

      "always grab the first available message" in {
        val handle = MultiReaderHelper.merge(va)

        val messages = new ArrayBuffer[ReadMessage]
        handle.messages foreach { messages += _ }

        // stripe some messages across
        val sentMessages = 0 until N*100 map { _ => mock[ReadMessage] }

        messages must beEmpty
        sentMessages.zipWithIndex foreach { case (m, i) =>
          handles(i % handles.size)._messages ! m
        }

        messages must be_==(sentMessages)
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

      "propagate closes" in {
        handles foreach { h => there was no(h).close() }
        val handle = MultiReaderHelper.merge(va)
        handle.close()
        handles foreach { h => there was one(h).close() }
      }

      "propagate errors when everything's errored out" in {
        val handle = MultiReaderHelper.merge(va)
        val e = handle.error.sync()
        handles foreach { h =>
          e.isDefined must beFalse
          h._error ! new Exception("sad panda")
        }

        e.isDefined must beTrue
        Await.result(e) must be(AllHandlesDiedException)
      }
    }

    "Var[Addr]-based cluster" in {
      val N = 3
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
          result.hosts(host) returns mockHostClientBuilder

          val queues = hostQueuesMap(host)
          val factory = new ServiceFactory[Command, Response] {
            // use an executor so readReliably doesn't block waiting on an empty queue
            def apply(conn: ClientConnection) =
              Future.value(newKestrelService(Some(executor), queues))
            def close(deadline: Time) = Future.Done
            override def toString = "ServiceFactory for %s".format(host)
          }
          mockHostClientBuilder.buildFactory() returns factory
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

      "read messages from a ready cluster" in {
        val va = Var(Addr.Bound(hosts: _*))
        val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
        }

        messages must eventually(be_==(sentMessages.toSet))
      }


      "read messages as cluster hosts are added" in {
        val va = Var(Addr.Bound(hosts.head))
        val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
        }

        // 0, 3, 6 ...
        messages must eventually(be_==(sentMessages.grouped(N).map { _.head }.toSet))
        messages.clear()

        va.update(Addr.Bound(hosts: _*))

        // 1, 2, 4, 5, ...
        messages must eventually(be_==(sentMessages.grouped(N).map { _.tail }.flatten.toSet))
      }

      "read messages as cluster hosts are removed" in {
        var mutableHosts: Seq[SocketAddress] = hosts
        val va = Var(Addr.Bound(mutableHosts: _*))
        val rest = hosts.tail.reverse
        val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()

        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
        }

        messages must eventually(be_==(sentMessages.toSet))
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
          messages must eventually(be_==(sentMessages.grouped(N).map { _.take(expectFirstN) }.flatten.toSet))
        }
      }

      "wait for cluster to become ready before snapping initial hosts" in {
        val va = Var(Addr.Bound())
        val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
        val messages = configureMessageReader(handle)
        val error = handle.error.sync()
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set(queueName, Time.now, m)))
        }

        messages must beEmpty // cluster not ready
        error.isDefined must beFalse

        va.update(Addr.Bound(hosts: _*))

        messages must eventually(be_==(sentMessages.toSet))
      }

      "report an error if all hosts are removed" in {
        val va = Var(Addr.Bound(hosts: _*))
        val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
        val error = handle.error.sync()
        va.update(Addr.Bound())

        error.isDefined must beTrue
        Await.result(error) must be(AllHandlesDiedException)
      }

      "propagate exception if cluster fails" in {
        val ex = new Exception("uh oh")
        val va: Var[Addr] with Updatable[Addr] = Var(Addr.Bound(hosts: _*))
        val handle = MultiReader(va, queueName).clientBuilder(mockClientBuilder).build()
        val error = handle.error.sync()
        va.update(Addr.Failed(ex))

        error.isDefined must beTrue
        Await.result(error) must be(ex)
      }
    }

    "[deprecated] dynamic SocketAddress cluster" in {
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
          result.hosts(host) returns mockHostClientBuilder

          val queues = hostQueuesMap(host)
          val factory = new ServiceFactory[Command, Response] {
            // use an executor so readReliably doesn't block waiting on an empty queue
            def apply(conn: ClientConnection) = Future(newKestrelService(Some(executor), queues))
            def close(deadline: Time) = Future.Done
            override def toString = "ServiceFactory for %s".format(host)
          }
          mockHostClientBuilder.buildFactory() returns factory
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

      "read messages from a ready cluster" in {
        val cluster = new DynamicCluster[SocketAddress](hosts)
        val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
        }

        messages must eventually(be_==(sentMessages.toSet))
      }

      "read messages as cluster hosts are added" in {
        val (host, rest) = (hosts.head, hosts.tail)
        val cluster = new DynamicCluster[SocketAddress](List(host))
        val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
        }

        // 0, 3, 6 ...
        messages must eventually(be_==(sentMessages.grouped(N).map { _.head }.toSet))
        messages.clear()

        rest.foreach { host => cluster.add(host) }

        // 1, 2, 4, 5, ...
        messages must eventually(be_==(sentMessages.grouped(N).map { _.tail }.flatten.toSet))
      }

      "read messages as cluster hosts are removed" in {
        val cluster = new DynamicCluster[SocketAddress](hosts)
        val rest = hosts.tail
        val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()

        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
        }

        messages must eventually(be_==(sentMessages.toSet))

        rest.reverse.zipWithIndex.foreach { case (host, hostIndex) =>
          messages.clear()
          cluster.del(host)

          // write to all 3
          sentMessages.zipWithIndex foreach { case (m, i) =>
            Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
          }

          // expect fewer to be read on each pass
          val expectFirstN = N - hostIndex - 1
          messages must eventually(be_==(sentMessages.grouped(N).map { _.take(expectFirstN) }.flatten.toSet))
        }
      }

      "wait for cluster to become ready before snapping initial hosts" in {
        val cluster = new DynamicCluster[SocketAddress](Seq())
        val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
        val messages = configureMessageReader(handle)
        val errors = handle.error?
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }
        messages must beEmpty

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
        }

        messages must beEmpty // cluster not ready
        errors.isDefined must beFalse

        hosts.foreach { host => cluster.add(host) }

        messages must eventually(be_==(sentMessages.toSet))
      }

      "report an error if all hosts are removed" in {
        val cluster = new DynamicCluster[SocketAddress](hosts)
        val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()
        val e = (handle.error?)
        hosts.foreach { host => cluster.del(host) }

        e.isDefined must beTrue
        Await.result(e) must be(AllHandlesDiedException)
      }

      "silently handle the removal of a host that was never added" in {
        val cluster = new DynamicCluster[SocketAddress](hosts)
        val handle = MultiReader(cluster, "the_queue").clientBuilder(mockClientBuilder).build()

        val messages = configureMessageReader(handle)
        val sentMessages = 0 until N*10 map { i => "message %d".format(i) }

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
        }
        messages must eventually(be_==(sentMessages.toSet))
        messages.clear()

        cluster.del(InetSocketAddress.createUnresolved("10.0.0.100", 22133))

        sentMessages.zipWithIndex foreach { case (m, i) =>
          Await.result(services(i % services.size).apply(Set("the_queue", Time.now, m)))
        }
        messages must eventually(be_==(sentMessages.toSet))
      }
    }
  }
}
