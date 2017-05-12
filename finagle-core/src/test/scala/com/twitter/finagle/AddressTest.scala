package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.FunSuite

class AddressTest extends FunSuite {
  test("constructor with no host points to localhost") {
    Address(8080) == Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080))
  }

  test("OctetOrdering is nop for unresolved addresses") {
    val unresolvedAddresses: Seq[Address.Inet] = (10 until 0 by -1).map { i =>
      Address.Inet(InetSocketAddress.createUnresolved(s"address-$i", 0),
        Addr.Metadata.empty)
    }
    val sorted = unresolvedAddresses.sorted(Address.OctetOrdering)
    sorted.indices.foreach { i =>
      assert(unresolvedAddresses(i) == sorted(i))
    }
  }

  test("OctetOrdering") {
    val size = 10
    val addresses: Seq[Address.Inet] = (size until 0 by -1).map { i =>
      val inet = InetAddress.getByAddress(Array[Byte](10, 0, 0, i.toByte))
      Address.Inet(new InetSocketAddress(inet, 0), Addr.Metadata.empty)
    }

    val sorted = addresses.sorted(Address.OctetOrdering)
    sorted.indices.foreach { i =>
      assert(addresses(i) == sorted(size - i - 1))
    }

    val failed = Address.Failed(new Exception)
    val withFailed = failed +: addresses
    assert(withFailed.sorted(Address.OctetOrdering).last == failed)

    val sf = exp.Address.ServiceFactory(ServiceFactory.const[Int, Int] {
      Service.mk[Int, Int] { _ => ??? }
    }, Addr.Metadata.empty)
    val withSf = sf +: addresses
    assert(withSf.sorted(Address.OctetOrdering).last == sf)
  }
}
