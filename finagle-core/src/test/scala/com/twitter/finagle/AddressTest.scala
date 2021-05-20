package com.twitter.finagle

import com.twitter.util.Future
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class AddressTest extends AnyFunSuite {

  def assertFactory[Req, Rep](address: Address, factory: ServiceFactory[Req, Rep]): Unit =
    address match {
      case Address.ServiceFactory(sf, _) => assert(sf == factory)
      case _ => fail(s"$address is not an Address.ServiceFactory")
    }

  def assertMetadata(address: Address, metadata: Addr.Metadata): Unit =
    address match {
      case Address.ServiceFactory(_, md) => assert(md == metadata)
      case _ => fail(s"$address is not an Address.ServiceFactory")
    }

  test("constructor with no host points to localhost") {
    assert(Address(8080) == Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080)))
  }

  test("hashOrdering") {
    val addr1 = Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8080))
    val addr2 = Address(InetSocketAddress.createUnresolved("btbb", 10))
    val addr3 = Address(new InetSocketAddress(InetAddress.getLoopbackAddress, 8090))
    val ordering = Address.hashOrdering(System.currentTimeMillis().toInt)

    val sorted = Seq(addr1, addr2, addr3).toVector.sorted(ordering)

    val cmp13 = ordering.compare(sorted(0), sorted(2))
    val cmp12 = ordering.compare(sorted(0), sorted(1))
    val cmp23 = ordering.compare(sorted(1), sorted(2))
    assert(cmp13 == 0 && cmp12 == 0 && cmp23 == 0 || cmp13 < 0 && (cmp12 < 0 || cmp23 < 0))
  }

  test("can be created via a ServiceFactory") {
    val service: Service[String, String] = Service.mk(_ => Future.value("Whatever"))
    val sf: ServiceFactory[String, String] = ServiceFactory.const(service)

    // Without Metadata
    val address1: Address = Address.ServiceFactory(sf)
    assertFactory(address1, sf)
    assertMetadata(address1, Addr.Metadata.empty)

    // With Metadata
    val metadata = Addr.Metadata("a" -> "b")
    val address2: Address = Address.ServiceFactory(sf, metadata)
    assertFactory(address2, sf)
    assertMetadata(address2, metadata)

    // Without Metadata via Address.apply
    val address3: Address = Address(sf)
    assertFactory(address3, sf)
    assertMetadata(address3, Addr.Metadata.empty)
  }
}
