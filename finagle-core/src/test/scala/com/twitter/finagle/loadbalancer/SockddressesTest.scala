package com.twitter.finagle.loadbalancer

import com.twitter.finagle.WeightedSocketAddress
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SocketAddressesTest extends FunSuite {
  val sa = new SocketAddress {}
  val inetsa = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
  val wsa = WeightedSocketAddress(sa, 2.0)
  val wsa2 = WeightedSocketAddress(inetsa, 3.0)
  val replicator = SocketAddresses.replicate(2)
  val isas = replicator(inetsa)

  test("replicas generates replicated address") {
    val replicator = SocketAddresses.replicate(3)

    val replicas = replicator(sa)
    assert(replicas.size == 3)
    replicas.foreach { addr =>
      assert(SocketAddresses.unwrap(addr) == sa)
    }

    val weightedreplicas = replicator(wsa)
    assert(weightedreplicas.size == 3)
    weightedreplicas.foreach { addr =>
      withClue("keeps weight: ") {
        val (_, 2.0) = WeightedSocketAddress.extract(addr)
      }
      withClue("wraps the underlying address: ") {
        assert(SocketAddresses.unwrap(addr) == sa)
      }
    }

    withClue("does not support nested replicas: ") {
      isas.foreach { replica =>
        replicator(replica) == Set(replica)
      }
    }
  }

  test("unwraps address") {
    assert(SocketAddresses.unwrap(sa) == sa)
    assert(SocketAddresses.unwrap(wsa) == sa)
    replicator(wsa).foreach { addr =>
      assert(SocketAddresses.unwrap(addr) == sa)
    }
    replicator(wsa).foreach {  replica => // Weighted(replicated(sa), w)
      (SocketAddresses.unwrap(WeightedSocketAddress(replica, 2.0)) == sa)
    }
  }
}

