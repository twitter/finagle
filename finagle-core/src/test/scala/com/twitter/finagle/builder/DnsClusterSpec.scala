package com.twitter.finagle.builder

import com.twitter.finagle.integration.StringCodec
import com.twitter.conversions.time._
import com.twitter.finagle.{ ClientCodecConfig, Codec, CodecFactory, Service }
import com.twitter.util._
import java.net.{ InetAddress, InetSocketAddress, SocketAddress, UnknownHostException }
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.{ DelimiterBasedFrameDecoder, Delimiters }
import org.jboss.netty.handler.codec.string.{ StringDecoder, StringEncoder }
import org.jboss.netty.util.CharsetUtil
import org.scalatest.FunSpec
import org.scalatest.matchers.MustMatchers

object DnsClusterSpec {
  def makeServer(name: String, f: String => String) = {
    val sillyService = new Service[String, String] {
      def apply(request: String) = Future(f(request))
    }

    ServerBuilder()
      .codec(StringCodec)
      .bindTo(new InetSocketAddress(0)) // ephemeral port
      .name(name)
      .build(sillyService)
  }

  def makeClient(cluster: DnsCluster) =
    ClientBuilder()
      .cluster(cluster)
      .codec(StringCodec)
      .hostConnectionLimit(10)
      .build()

}

class DnsClusterSpec extends FunSpec with MustMatchers {
  import DnsClusterSpec._

  describe("DnsCluster") {
    it("should be able to block till server set is ready") {
      Time.withCurrentTimeFrozen { tc =>
        val server = makeServer("reverse", s => s.reverse)

        val timer = new MockTimer()

        val cluster = new DnsCluster("block.until.ready.test", 80, 10.seconds, timer) {
          override val resolveHost = Promise[Set[SocketAddress]]()
        }

        val client = makeClient(cluster)

        val response = client("hello\n")

        cluster.ready.isDefined must be(false)

        cluster.resolveHost.setValue((Set(server.localAddress)))

        tc.advance(10.seconds)
        timer.tick()

        cluster.ready.isDefined must be(true)

        Await.result(response, 1.second) must be("olleh")
      }
    }

    it("should be able to recognize a DNS change") {
      Time.withCurrentTimeFrozen { tc =>
        val timer = new MockTimer()

        val server1 = makeServer("server1", _ + " server1")
        val server2 = makeServer("server2", _ + " server2")

        val cluster = new DnsCluster("dns.change.test",
          80, 10.seconds, timer = timer) {

          @volatile
          var currentServer: Server = server1

          override def resolveHost =
            Future.value(Set(currentServer.localAddress))
        }

        val client = makeClient(cluster)

        cluster.currentServer = server1

        timer.tick()
        Await.result(client("hello\n"), 1.second) must be("hello server1")

        cluster.currentServer = server2
        tc.advance(10.seconds)
        timer.tick()

        Await.result(client("hello\n"), 1.second) must be("hello server2")
      }
    }

    it("should handle multiple random DNS chages") {
      Time.withCurrentTimeFrozen { tc =>

        val timer = new MockTimer()

        val addresses = for (n <- 1 to 10) yield {
          var bytes = Array[Byte](10, 0, 0, n.toByte)
          new InetSocketAddress(InetAddress.getByAddress(bytes), 80)
        }

        val cluster = new DnsCluster("multi.dns.changes.test",
          80, 10.seconds, timer) {

          @volatile
          var current: Set[SocketAddress] = _
          override def resolveHost = Future.value(current)
        }

        val rnd = new scala.util.Random
        val (seq, changes) = cluster.snap
        var current = seq.toSet
        changes foreach { spool =>
          spool foreach {
            case Cluster.Add(elem) => current += elem
            case Cluster.Rem(elem) => current -= elem
          }
        }

        for (i <- 0 to 100) {
          cluster.current = Set(rnd.shuffle(addresses).take(2): _*)

          tc.advance(10.seconds)
          timer.tick()
          current must equal(cluster.current)

          current = cluster.current
        }
      }
    }

    it("should handle UnknownHostException") {
      Time.withCurrentTimeFrozen { tc =>
        val timer = new MockTimer()

        val cluster = new DnsCluster("unknown.host.exception.test",
          80, 10.seconds, timer) {

          override def blockingDnsCall: Set[SocketAddress] =
            throw new UnknownHostException("blah")
        }

        cluster.ready.isDefined must be(false)

        tc.advance(20.seconds)

        timer.tick()

        cluster.ready.isDefined must be(false)
      }
    }
  }
}
