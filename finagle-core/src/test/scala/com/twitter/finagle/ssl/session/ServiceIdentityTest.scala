package com.twitter.finagle.ssl.session

import com.twitter.finagle.ssl.session.ServiceIdentity.DnsServiceIdentity
import com.twitter.finagle.ssl.session.ServiceIdentity.GeneralName
import com.twitter.finagle.ssl.session.ServiceIdentity.IpServiceIdentity
import com.twitter.finagle.ssl.session.ServiceIdentity.IpTag
import com.twitter.finagle.ssl.session.ServiceIdentity.UriServiceIdentity
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

class ServiceIdentityTest extends AnyFunSuite with MockitoSugar {

  case class TestCase(name: String, tag: Int, subtype: Class[ServiceIdentity])
  test("ServiceIdentity can be constructed from URI") {
    val name = "some:uri"
    val identity = ServiceIdentity(GeneralName(ServiceIdentity.UriTag, name))
    assert(identity.isInstanceOf[UriServiceIdentity])
    assert(identity.name == name)
  }

  test("ServiceIdentity can be constructed from DNS") {
    val name = "some.dns"
    val identity = ServiceIdentity(GeneralName(ServiceIdentity.DnsTag, name))
    assert(identity.isInstanceOf[DnsServiceIdentity])
    assert(identity.name == name)
  }

  test("ServiceIdentity can be constructed from IP") {
    val name = "127.0.0.1"
    val identity = ServiceIdentity(GeneralName(IpTag, name))
    assert(identity.isInstanceOf[IpServiceIdentity])
    assert(identity.name == name)
  }
}
