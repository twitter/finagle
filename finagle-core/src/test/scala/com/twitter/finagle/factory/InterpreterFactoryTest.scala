package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.util.{Await, Future, Time, Var, Activity}
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
class InterpreterFactoryTest extends FunSuite with MockitoSugar {
  def anonNamer() = new Namer {
    def lookup(prefix: Path): Activity[NameTree[Path]] =
      Activity.value(NameTree.Neg)
  }

  test("Caches factories") {
    var namer = Namer.global
    val name = UninterpretedName(
      () => namer,
      NameTree.read("/$/inet//8080"))

    var news = 0
    var closes = 0

    val newFactory: Var[Addr] => ServiceFactory[Unit, Var[Addr]] = 
      addr => new ServiceFactory[Unit, Var[Addr]] {
        news += 1
        def apply(conn: ClientConnection) = Future.value(new Service[Unit, Var[Addr]] {
          def apply(_unit: Unit) = Future.value(addr)
        })

        def close(deadline: Time) = {
          closes += 1
          Future.Done
        }
      }

    val factory = new InterpreterFactory(name, newFactory, maxCacheSize = 2)
    
    def newWith(localNamer: Namer): Service[Unit, Var[Addr]] = {
      namer = localNamer
      Await.result(factory())
    }
    
    val n1, n2, n3, n4 = anonNamer()

    assert(news === 0)
    Await.result(newWith(n1).close() before newWith(n1).close())
    assert(news === 1)
    assert(closes === 0)

    val s2 = newWith(n2)
    assert(news === 2)
    assert(closes === 0)
    
    // This should evict n1
    val s3 = newWith(n3)
    assert(news === 3)
    assert(closes === 1)
    

    // n2, n3 are outstanding, so additional requests
    // should hit the one-shot path.
    val s1 = newWith(n1)
    assert(news === 4)
    assert(closes === 1)
    // Closing this should close the factory immediately.
    s1.close()
    assert(closes === 2)

    Await.result(newWith(n2).close() before newWith(n3).close())
    assert(news === 4)
    assert(closes === 2)
  }
  
  // Evicts max idle
}
