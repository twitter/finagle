package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.finagle.factory.ServiceFactoryCache
import com.twitter.util.Await
import com.twitter.util.Activity
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class DynNameFactoryTest extends AnyFunSuite with MockitoSugar {
  private trait Ctx {
    val cache = mock[ServiceFactoryCache[NameTree[Name.Bound], String, String]]
    val svc = mock[Service[String, String]]
    val (name, namew) = Activity[NameTree[Name.Bound]]()
    val dyn = new DynNameFactory[String, String](name, cache)
  }

  test("DynNameFactory is Busy when name is unresolved")(new Ctx {
    intercept[IllegalStateException] { name.sample() }
    assert(dyn.status == Status.Busy)
  })

  test("DynNameFactory is Closed when name resolution fails")(new Ctx {
    assert(dyn.status == Status.Busy)
    namew.notify(Throw(new Exception("boom")))
    assert(dyn.status == Status.Closed)
  })

  test("DynNameFactory is Closed after closing")(new Ctx {
    assert(dyn.status == Status.Busy)
    Await.ready(dyn.close())
    assert(dyn.status == Status.Closed)
  })

  test("DynNameFactory reflects status of underlying cached service factory")(
    for (status <- Seq(Status.Closed, Status.Busy, Status.Open)) {
      new Ctx {
        when(cache.status(any[NameTree[Name.Bound]])).thenReturn(status)
        namew.notify(Return(NameTree.Leaf(Name.empty)))
        assert(dyn.status == status)
      }
    }
  )

  test("queue requests until name is nonpending (ok)")(new Ctx {
    when(cache(any[NameTree[Name.Bound]], any[ClientConnection])).thenReturn(Future.value(svc))

    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)

    namew.notify(Return(NameTree.Leaf(Name.empty)))

    assert(f1.poll == Some(Return(svc)))
    assert(f2.poll == Some(Return(svc)))

    Await.result(f1)("foo")
    Await.result(f1)("bar")
    Await.result(f2)("baz")
  })

  test("queue requests until name is nonpending (fail)")(new Ctx {
    when(cache(any[NameTree[Name.Bound]], any[ClientConnection])).thenReturn(Future.never)

    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)

    val exc = new Exception
    namew.notify(Throw(exc))

    assert(f1.poll == Some(Throw(Failure(exc, FailureFlags.Naming))))
    assert(f2.poll == Some(Throw(Failure(exc, FailureFlags.Naming))))
  })

  test("dequeue interrupted requests")(new Ctx {
    when(cache(any[NameTree[Name.Bound]], any[ClientConnection])).thenReturn(Future.never)

    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)

    val exc = new Exception
    f1.raise(exc)

    f1.poll match {
      case Some(Throw(cce: CancelledConnectionException)) =>
        assert(cce.getCause == exc)
      case _ => fail()
    }
    assert(f2.poll == None)

    namew.notify(Return(NameTree.Leaf(Name.empty)))
    assert(f2.poll == None)
  })
}
