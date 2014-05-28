package com.twitter.finagle.factory

import com.twitter.finagle._
import com.twitter.util.{Await, Future, Time, Var, Activity, Promise, Throw, Return}
import java.net.{InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{never, times, verify, when}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import com.twitter.finagle.stats._

@RunWith(classOf[JUnitRunner])
class BindingFactoryTest extends FunSuite with MockitoSugar with BeforeAndAfter {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("""
      /test1010=>/$/inet//1010
    """)
  }
  
  after {
    Dtab.base = saveBase
  }

  def anonNamer() = new Namer {
    def lookup(path: Path): Activity[NameTree[Name]] =
      Activity.value(NameTree.Neg)
  }
  
  trait Ctx {
    val imsr = new InMemoryStatsReceiver

    val path = Path.read("/foo/bar")

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

    val factory = new BindingFactory(
      path, newFactory,
      statsReceiver = imsr,
      maxNamerCacheSize = 2, 
      maxNameCacheSize = 2)

    def newWith(localDtab: Dtab): Service[Unit, Var[Addr]] = {
      Dtab.unwind {
        Dtab.local = localDtab
        Await.result(factory())
      }
    }
  }
  
  test("Uses Dtab.base") (new Ctx {
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = Await.result(s1(()))
    assert(v1.sample() === Addr.Bound(new InetSocketAddress(1010)))

    s1.close()
  })

  test("Caches namers") (new Ctx {

    val n1 = Dtab.read("/foo/bar=>/$/inet//1")
    val n2 = Dtab.read("/foo/bar=>/$/inet//2")
    val n3 = Dtab.read("/foo/bar=>/$/inet//3")
    val n4 = Dtab.read("/foo/bar=>/$/inet//4")

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
  })

  test("Caches names") (new Ctx {
    val n1 = Dtab.read("/foo/bar=>/$/inet//1; /bar/baz=>/$/nil")
    val n2 = Dtab.read("/foo/bar=>/$/inet//1")
    val n3 = Dtab.read("/foo/bar=>/$/inet//2")
    val n4 = Dtab.read("/foo/bar=>/$/inet//3")

    assert(news === 0)
    Await.result(newWith(n1).close() before newWith(n1).close())
    assert(news === 1)
    assert(closes === 0)

    Await.result(newWith(n2).close())
    assert(news === 1)
    assert(closes === 0)

    Await.result(newWith(n3).close())
    assert(news === 2)
    assert(closes === 0)

    Await.result(newWith(n4).close())
    assert(news === 3)
    assert(closes === 1)

    Await.result(newWith(n3).close())
    assert(news === 3)
    assert(closes === 1)
    
    Await.result(newWith(n1).close())
    assert(news === 4)
    assert(closes === 2)
            
    Await.result(newWith(n2).close())
    assert(news === 4)
    assert(closes === 2)
  })
}

@RunWith(classOf[JUnitRunner])
class DynNameFactoryTest extends FunSuite with MockitoSugar {
  private trait Ctx {
    val newService = mock[(Name.Bound, ClientConnection) => Future[Service[String, String]]]
    val svc = mock[Service[String, String]]
    val (name, namew) = Activity[Name.Bound]()
    val dyn = new DynNameFactory[String, String](name, newService)
  }

  test("queue requests until name is nonpending (ok)") (new Ctx {
    when(newService(any[Name.Bound], any[ClientConnection])).thenReturn(Future.value(svc))

    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)

    namew.notify(Return(Name.empty))

    assert(f1.poll === Some(Return(svc)))
    assert(f2.poll === Some(Return(svc)))
  })
  
  test("queue requests until name is nonpending (fail)") (new Ctx {
    when(newService(any[Name.Bound], any[ClientConnection])).thenReturn(Future.never)

    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)
    
    val exc = new Exception
    namew.notify(Throw(exc))
    
    assert(f1.poll === Some(Throw(exc)))
    assert(f2.poll === Some(Throw(exc)))
  })
  
  test("dequeue interrupted requests") (new Ctx {
    when(newService(any[Name.Bound], any[ClientConnection])).thenReturn(Future.never)
    
    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)
    
    val exc = new Exception
    f1.raise(exc)
    
    f1.poll match {
      case Some(Throw(cce: CancelledConnectionException)) =>
        assert(cce.getCause === exc)
      case _ => fail()
    }
    assert(f2.poll === None)

    namew.notify(Return(Name.empty))
    assert(f2.poll === None)
  })
} 

