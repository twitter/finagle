package com.twitter.finagle

import com.twitter.finagle.service.ConstantService
import com.twitter.finagle.service.ServiceFactoryRef
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class ServiceFactoryTest extends AnyFunSuite {

  class TestServiceFactory extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection): Future[Service[Int, Int]] =
      Future.value(new ConstantService[Int, Int](Future.value(7)))

    def close(deadline: Time): Future[Unit] = Future.Done

    def status: Status = Status.Open
  }

  case class TestProxy(_self: ServiceFactory[Int, Int]) extends ServiceFactoryProxy(_self)
  case class TestProxyRef(_self: ServiceFactory[Int, Int]) extends ServiceFactoryRef(_self)

  val Seq(sf1, sf2, sf3, sf4) = (0 until 4).map(_ => new TestServiceFactory)
  val Seq(sfp1, sfp2, sfp3, sfp4) =
    Seq(sf1, sf2, sf3, sf4).map(new ServiceFactoryProxy[Int, Int](_) {})
  val Seq(sfr1, sfr2, sfr3, sfr4) =
    Seq(sfp1, sfp2, sfp3, sfp4).map(new ServiceFactoryRef[Int, Int](_))

  val sfBundle = Seq(
    ("ServiceFactory", sf1, sf2, sf3, sf4),
    ("ServiceFactoryProxy", sfp1, sfp2, sfp3, sfp4),
    ("ServiceFactoryRef", sfr1, sfr2, sfr3, sfr4)
  )

  sfBundle.map {
    case (name, sf1, sf2, sf3, sf4) =>
      test(s"$name Set equality") {
        testSetEquality(sf1, sf2, sf3, sf4)
      }

      test(s"$name Map Keys equality") {
        val m1: Map[ServiceFactory[Int, Int], String] = Map(sf1 -> "sf1", sf2 -> "sf2")
        val m2: Map[ServiceFactory[Int, Int], String] = Map(sf1 -> "sf1", sf2 -> "sf2")
        assert(m1 == m2 && m2 == m1)
        assert(m1.keys == m2.keys)
        assert(m1.keySet == m2.keySet)
      }

      test(s"$name uses reference, not structural equality") {
        assert(sf1 != sf2 && sf2 != sf1)
        if (name == "ServiceFactoryRef") {
          // HashCode should be consistent after update()
          val sfrHashCodeInit: Int = sf1.hashCode
          val sf3: ServiceFactory[Int, Int] = new TestServiceFactory
          sf1.asInstanceOf[ServiceFactoryRef[Int, Int]].update(sf3)
          val sfrHashCodeUpdated: Int = sf1.hashCode
          assert(sfrHashCodeInit == sfrHashCodeUpdated)
          assert(sf1 != sf3 && sf3 != sf1)
        }
      }
  }

  test("ServiceFactoryProxy toString") {
    val sf: ServiceFactory[Int, Int] = new TestServiceFactory
    val sfp: TestProxy = TestProxy(sf)
    val expectedString: String = "com.twitter.finagle.ServiceFactoryTest$" +
      "TestProxy(com.twitter.finagle.ServiceFactoryTest$TestServiceFactory)"
    assert(sfp.toString.equals(expectedString))
  }

  test("ServiceFactoryRef toString") {
    val sf: ServiceFactory[Int, Int] = new TestServiceFactory
    val sfr: TestProxyRef = TestProxyRef(sf)
    val expectedString: String = "com.twitter.finagle.ServiceFactoryTest$" +
      "TestProxyRef(com.twitter.finagle.ServiceFactoryTest$TestServiceFactory)"
    assert(sfr.toString.equals(expectedString))
  }

  test("Set Inequality test with ServiceFactory, ServiceFactoryProxy, and ServiceFactoryRef") {
    val sf1: ServiceFactory[Int, Int] = new TestServiceFactory
    val sf2: ServiceFactory[Int, Int] = new TestServiceFactory
    val sfp1: ServiceFactoryProxy[Int, Int] = new ServiceFactoryProxy[Int, Int](sf1) {}
    val sfp2: ServiceFactoryProxy[Int, Int] = new ServiceFactoryProxy[Int, Int](sf2) {}
    val sfr1: ServiceFactoryRef[Int, Int] = new ServiceFactoryRef[Int, Int](sfp1)
    val sfr2: ServiceFactoryRef[Int, Int] = new ServiceFactoryRef[Int, Int](sfp2)

    // with 2 elements
    val s21: Set[ServiceFactory[Int, Int]] = Set(sf1, sf2)
    val s22: Set[ServiceFactory[Int, Int]] = Set(sf2, sfp1)
    val s23: Set[ServiceFactory[Int, Int]] = Set(sfp1, sfp2)
    val s24: Set[ServiceFactory[Int, Int]] = Set(sfp2, sfr1)
    val s25: Set[ServiceFactory[Int, Int]] = Set(sfr1, sfr2)
    val s26: Set[ServiceFactory[Int, Int]] = Set(sfr2, sf1)
    assert(s21 != s22 && s22 != s23 && s23 != s24 && s25 != s26 && s26 != s21)

    // with 3 elements
    val s31: Set[ServiceFactory[Int, Int]] = Set(sf1, sf2, sfp1)
    val s32: Set[ServiceFactory[Int, Int]] = Set(sf2, sfp1, sfp2)
    val s33: Set[ServiceFactory[Int, Int]] = Set(sfp1, sfp2, sfr1)
    val s34: Set[ServiceFactory[Int, Int]] = Set(sfp2, sfr1, sfr2)
    val s35: Set[ServiceFactory[Int, Int]] = Set(sfr1, sfr2, sf1)
    assert(s31 != s32 && s32 != s33 && s33 != s34 && s34 != s35 && s35 != s31)

    // with 4 elements
    val s41: Set[ServiceFactory[Int, Int]] = Set(sf1, sf2, sfp1, sfp2)
    val s42: Set[ServiceFactory[Int, Int]] = Set(sf2, sfp1, sfp2, sfr1)
    val s43: Set[ServiceFactory[Int, Int]] = Set(sfp1, sfp2, sfr1, sfr2)
    val s44: Set[ServiceFactory[Int, Int]] = Set(sfp2, sfr1, sfr2, sf1)
    assert(s41 != s42 && s42 != s43 && s43 != s41 && s44 != s41)

    // with 5 elements
    val s51: Set[ServiceFactory[Int, Int]] = Set(sf1, sf2, sfp1, sfp2, sfr1)
    val s52: Set[ServiceFactory[Int, Int]] = Set(sf2, sfp1, sfp2, sfr1, sfr2)
    val s53: Set[ServiceFactory[Int, Int]] = Set(sfp1, sfp2, sfr1, sfr2, sf1)
    assert(s51 != s52 && s52 != s53 && s53 != s51)

    // with 6 elements
    val s61: Set[ServiceFactory[Int, Int]] = Set(sf1, sf2, sfp1, sfp2, sfr1, sfr2)
    val s62: Set[ServiceFactory[Int, Int]] = Set(sf2, sfp1, sfp2, sfr1, sfr2, sf1)
    assert(s61 == s62 && s62 == s61)
  }

  private[this] def testSetEquality(
    sf1: ServiceFactory[Int, Int],
    sf2: ServiceFactory[Int, Int],
    sf3: ServiceFactory[Int, Int],
    sf4: ServiceFactory[Int, Int]
  ): Unit = {
    var s1: Set[ServiceFactory[Int, Int]] = Set(sf1, sf2)
    var s2: Set[ServiceFactory[Int, Int]] = Set(sf2, sf1)
    assert(s1 == s2 && s2 == s1)

    // add the same sf to both sets
    s1 += sf3
    s2 += sf3
    assert(s1 == s2 && s2 == s1)

    // add duplicate sf to s1 (shouldn't succeed)
    s1 += sf1
    assert(s1 == s2 && s2 == s1)

    // drop the same sf from both sets
    s1 -= sf2
    s2 -= sf2
    assert(s1 == s2 && s2 == s1)

    // test Set diff operation
    val s3: Set[ServiceFactory[Int, Int]] = Set(sf2, sf3, sf4)
    assert(s3 != s1 && s1 != s3)
    assert((s1 &~ s3) == Set(sf1))
    assert((s3 &~ s1) == Set(sf2, sf4))
  }
}
