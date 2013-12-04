package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.util.{Await, Future, Time, Var}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{never, times, verify, when}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class RefineryTest extends FunSuite with MockitoSugar {
  def newCtx() = new {
    val defaultName = new Name {
      val addr = mock[Addr]
      def bind() = Var.value(addr)
      val reified = "fail!"
    }

    val ctx = new DtabCtx {
      var base = Dtab.empty.delegated("/", defaultName)
      var active: Option[Dtab] = None
      def apply() = active getOrElse base
      def update(dtab: Dtab) { active = Some(dtab) }
      def clear() { active = None }
    }

    val newFactory = mock[Name => ServiceFactory[Int, Int]]
    val default = mock[ServiceFactory[Int, Int]]

    when(default.close(any[Time])).thenReturn(Future.Done)
    when(default.isAvailable).thenReturn(true)
    when(newFactory(any[Name])).thenReturn(default)

    val factory = new Refinery(
      Name("/mypath"), newFactory, 
      ctx)
  }

  test("creates, proxies default factory with unbound group") {
    val CTX = newCtx()
    import CTX._

    val arg = ArgumentCaptor.forClass(classOf[Name])
    verify(newFactory, times(1)).apply(arg.capture())
    arg.getValue().bind() match {
      case Var.Sampled(defaultName.addr) => 
      case _ => fail()
    }

    val t = Time.Zero
    Await.result(factory.close(t))
    verify(default).close(t)

    assert(factory.isAvailable)
    verify(default).isAvailable
    
    // We don't create additional factories.
    factory(ClientConnection.nil)
    verify(newFactory, times(1)).apply(any[Name])
  }

  test("creates new factories for bound addresses") {
    val CTX = newCtx()
    import CTX._

    val newName = new Name {
      val addr = mock[Addr]
      def bind() = Var.value(addr)
      val reified = "fail!"
    }
    ctx.delegate("/mypath", newName)

    val factory1 = mock[ServiceFactory[Int, Int]]
    val service1 = mock[Service[Int, Int]]
    when(service1.close(any[Time])).thenReturn(Future.Done)
    when(factory1(any[ClientConnection]))
      .thenReturn(Future.value(service1))
    when(factory1.close(any[Time])).thenReturn(Future.Done)
    
    when(newFactory(any[Name])).thenAnswer(
      new Answer[ServiceFactory[Int, Int]] {
        def answer(invk: InvocationOnMock) = {
          invk.getArguments match {
            case Array(n: Name) =>
              n.bind() match {
                case Var.Sampled(defaultName.addr) => default
                case Var.Sampled(newName.addr) => factory1
              }
          }
        }
      }
    )

    val s = Await.result(factory())
    verify(newFactory, times(2)).apply(any[Name])

    verify(factory1).apply(any[ClientConnection])
    verify(factory1, times(0)).close(any[Time])
    verify(service1, times(0)).close(any[Time])
    Await.result(s.close(Time.Zero))
    verify(service1, times(1)).close(any[Time])
    verify(factory1, times(1)).close(Time.Zero)
  }


  test("reestablishes bases") {
    val CTX = newCtx()
    import CTX._
    
    val service = mock[Service[Int, Int]]
    when(factory(any[ClientConnection]))
      .thenReturn(Future.value(service))
    
    verify(newFactory, times(1)).apply(any[Name])
    Await.result(factory())
    verify(newFactory, times(1)).apply(any[Name])
    verify(default, times(0)).close(any[Time])
    ctx.base = Dtab.empty
    Await.result(factory())
    verify(default, times(1)).close(any[Time])
    verify(newFactory, times(2)).apply(any[Name])
  }
}
