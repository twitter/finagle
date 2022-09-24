package com.twitter.finagle.ssl.session

import com.twitter.finagle.ssl.session.ServiceIdentity.DnsServiceIdentity
import com.twitter.finagle.ssl.session.ServiceIdentity.IpServiceIdentity
import com.twitter.finagle.ssl.session.ServiceIdentity.UriServiceIdentity
import org.bouncycastle.asn1.x509.GeneralName
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class ServiceIdentityTest extends AnyFunSuite with MockitoSugar {

  case class TestCase(name: String, tag: Int, subtype: Class[ServiceIdentity])
  test("ServiceIdentity can be constructed from URI") {
    val name = "some:uri"
    val generalName = new GeneralName(6, name)
    val identity = ServiceIdentity(generalName)
    assert(identity.isInstanceOf[UriServiceIdentity])
    assert(identity.name == name)
  }

  test("ServiceIdentity can be constructed from DNS") {
    val name = "some.dns"
    val generalName = new GeneralName(2, name)
    val identity = ServiceIdentity(generalName)
    assert(identity.isInstanceOf[DnsServiceIdentity])
    assert(identity.name == name)
  }

  test("ServiceIdentity can be constructed from IP") {
    val name = "127.0.0.1"
    val generalName = new GeneralName(7, name)
    val identity = ServiceIdentity(generalName)
    assert(identity.isInstanceOf[IpServiceIdentity])
    assert(identity.name == "#7f000001")
  }
}
