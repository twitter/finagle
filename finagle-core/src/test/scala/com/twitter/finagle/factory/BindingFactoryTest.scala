package com.twitter.finagle.factory

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats._
import com.twitter.finagle.util.Rng
import com.twitter.util._
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class BindingFactoryTest extends FunSuite with MockitoSugar with BeforeAndAfter {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base ++= Dtab.read("""
      /test1010=>/$/inet/0/1010
    """)
  }

  after {
    Dtab.base = saveBase
  }

  def anonNamer() = new Namer {
    def lookup(path: Path): Activity[NameTree[Name]] =
      Activity.value(NameTree.Neg)
    def enum(prefix: Path): Activity[Dtab] =
      Activity.exception(new UnsupportedOperationException)
  }

  trait Ctx {
    var records = Seq.empty[(String, String)]
    def record(key: String, value: String) {
      records :+= key -> value
    }
    def expectTrace(expected: Seq[(String, String)]) {
      assert(expected == records)
    }

    val imsr = new InMemoryStatsReceiver

    val path = Path.read("/foo/bar")

    var news = 0
    var closes = 0

    val tcOpt: Option[TimeControl] = None

    lazy val newFactory: Name.Bound => ServiceFactory[Unit, Var[Addr]] =
      bound => new ServiceFactory[Unit, Var[Addr]] {
        news += 1
        def apply(conn: ClientConnection) = {
          tcOpt.foreach(_.advance(1234.microseconds))
          Future.value(Service.mk { _ => Future.value(bound.addr) })
        }

        def close(deadline: Time) = {
          closes += 1
          Future.Done
        }
      }

    lazy val factory = new BindingFactory(
      path,
      newFactory,
      statsReceiver = imsr,
      maxNamerCacheSize = 2,
      maxNameCacheSize = 2,
      record = record)

    def newWith(localDtab: Dtab): Service[Unit, Var[Addr]] = {
      Dtab.unwind {
        Dtab.local = localDtab
        Await.result(factory())
      }
    }
  }

  def mkFactory(st: Status) =
    (bound: Name.Bound) => new ServiceFactory[Unit, Var[Addr]] {
      def apply(conn: ClientConnection) =
        Future.value(Service.mk { _ => Future.exception(new Exception("nup")) })
      def close(deadline: Time) = Future.Done
      override def status = st
    }

  test("BindingFactory reflects status of underlying cached service factory") (
    for(status <- Seq(Status.Busy, Status.Open, Status.Closed)) {
      new Ctx {
        override lazy val newFactory = mkFactory(status)

        // no binding yet
        assert(factory.status === Status.Closed)

        Dtab.unwind {
          Dtab.local = Dtab.read("/foo/bar=>/test1010")
          assert(factory.status === status)
        }
      }
    }
  )

  test("stats") (Time.withCurrentTimeFrozen { tc =>
    new Ctx {
      override val tcOpt = Some(tc)

      val n1 = Dtab.read("/foo/bar=>/test1010")
      Await.result(newWith(n1).close() before newWith(n1).close())

      val expected = Map(
        Seq("bind_latency_us") -> Seq(1234.0, 1234.0),
        Seq("dtabcache", "misstime_ms") -> Seq(1.0),
        Seq("namecache", "misstime_ms") -> Seq(1.0),
        Seq("nametreecache", "misstime_ms") -> Seq(1.0)
      )

      assert(imsr.stats == expected)
    }
  })

  test("Uses Dtab.base") (new Ctx {
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = Await.result(s1(()))
    assert(v1.sample() === Addr.Bound(new InetSocketAddress(1010)))

    s1.close()
  })

  test("Respects Dtab.base changes after service factory creation") (new Ctx {
    // factory is already created here
    Dtab.base ++= Dtab.read("/test1010=>/$/inet/0/1011")
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = Await.result(s1(()))
    assert(v1.sample() === Addr.Bound(new InetSocketAddress(1011)))

    s1.close()
  })

  test("Includes path in NoBrokersAvailableException") (new Ctx {
    val noBrokers = intercept[NoBrokersAvailableException] {
      Await.result(factory())
    }

    assert(noBrokers.name === "/foo/bar")
    assert(noBrokers.localDtab === Dtab.empty)
  })

  test("Includes path and Dtab.local in NoBrokersAvailableException from name resolution") (new Ctx {
    val localDtab = Dtab.read("/baz=>/quux")

    val noBrokers = intercept[NoBrokersAvailableException] {
      newWith(localDtab)
    }

    assert(noBrokers.name === "/foo/bar")
    assert(noBrokers.localDtab === localDtab)
  })

  test("Includes path and Dtab.local in NoBrokersAvailableException from service creation") {
    val localDtab = Dtab.read("/foo/bar=>/test1010")

    val factory = new BindingFactory(
      Path.read("/foo/bar"),
      newFactory = { addr =>
        new ServiceFactory[Unit, Unit] {
          def apply(conn: ClientConnection) =
            Future.exception(new NoBrokersAvailableException("/foo/bar"))

          def close(deadline: Time) = Future.Done
        }
      })

    val noBrokers = intercept[NoBrokersAvailableException] {
      Dtab.unwind {
        Dtab.local = localDtab
        Await.result(factory())
      }
    }

    assert(noBrokers.name === "/foo/bar")
    assert(noBrokers.localDtab === localDtab)
  }

  test("Trace on success") (new Ctx {
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = Await.result(s1(()))
    s1.close()

    expectTrace(Seq(
      "namer.path" -> "/foo/bar",
      "namer.dtab.base" -> "/=>/#/com.twitter.finagle.namer.global;/test1010=>/$/inet/0/1010",
      "namer.tree" -> "/$/inet/0/1010",
      "namer.name" -> "/$/inet/0/1010"
    ))
  })

  test("Trace on exception") (new Ctx {
    val exc = new RuntimeException

    Dtab.base = new Dtab(Dtab.base) {
      override def lookup(path: Path) =
        Activity.exception(exc)
    }

    assert(intercept[Failure](Await.result(factory())).isFlagged(Failure.Naming))

    expectTrace(Seq(
      "namer.path" -> "/foo/bar",
      "namer.dtab.base" -> "/=>/#/com.twitter.finagle.namer.global;/test1010=>/$/inet/0/1010",
      "namer.failure" -> "java.lang.RuntimeException"
    ))
  })

  test("Trace on naming failure") (new Ctx {
    intercept[NoBrokersAvailableException] {
      Await.result(factory())
    }

    expectTrace(Seq(
      "namer.path" -> "/foo/bar",
      "namer.dtab.base" -> "/=>/#/com.twitter.finagle.namer.global;/test1010=>/$/inet/0/1010",
      "namer.tree" -> "~"
    ))
  })

  test("Trace on service creation failure") (new Ctx {
    val localDtab = Dtab.read("/foo/bar=>/test1010")

    val f = new BindingFactory(
      Path.read("/foo/bar"),
      newFactory = { addr =>
        new ServiceFactory[Unit, Unit] {
          def apply(conn: ClientConnection) =
            Future.exception(new NoBrokersAvailableException("/foo/bar"))

          def close(deadline: Time) = Future.Done
        }
      },
      record = record)

    intercept[NoBrokersAvailableException] {
      Dtab.unwind {
        Dtab.local = localDtab
        Await.result(f())
      }
    }

    expectTrace(Seq(
      "namer.path" -> "/foo/bar",
      "namer.dtab.base" -> "/=>/#/com.twitter.finagle.namer.global;/test1010=>/$/inet/0/1010",
      "namer.tree" -> "/$/inet/0/1010",
      "namer.name" -> "/$/inet/0/1010"
    ))
  })

  test("Caches namers") (new Ctx {
    val n1 = Dtab.read("/foo/bar=>/$/inet/0/1")
    val n2 = Dtab.read("/foo/bar=>/$/inet/0/2")
    val n3 = Dtab.read("/foo/bar=>/$/inet/0/3")
    val n4 = Dtab.read("/foo/bar=>/$/inet/0/4")

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
    val n1 = Dtab.read("/foo/bar=>/$/inet/0/1; /bar/baz=>/$/nil")
    val n2 = Dtab.read("/foo/bar=>/$/inet/0/1")
    val n3 = Dtab.read("/foo/bar=>/$/inet/0/2")
    val n4 = Dtab.read("/foo/bar=>/$/inet/0/3")

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

  test("BindingFactory.Module: filters with bound residual paths") {
    val module = new BindingFactory.Module[Path, Path] {
      protected[this] def boundPathFilter(path: Path) =
        Filter.mk { (in, service) => service(path ++ in) }
    }

    val name = Name.Bound(Var(Addr.Pending), "id", Path.read("/alpha"))

    val end = Stack.Leaf(Stack.Role("end"),
      ServiceFactory(() => Future.value(Service.mk[Path, Path](Future.value))))

    val params = Stack.Params.empty + BindingFactory.Dest(name)
    val factory = module.toStack(end).make(params)
    val service = Await.result(factory())
    val full = Await.result(service(Path.read("/omega")))
    assert(full === Path.read("/alpha/omega"))
  }

  test("BindingFactory.Module: replaces Dest for bound name") {
    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo => /$/inet/0/1")

    val verifyModule =
      new Stack.Module1[BindingFactory.Dest, ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the dest was set properly"

        def make(dest: BindingFactory.Dest, next: ServiceFactory[String, String]) = {
          dest match {
            case BindingFactory.Dest(bound: Name.Bound) =>
              assert(bound.id == Path.read("/$/inet/0/1"))
            case _ => fail()
          }
          ServiceFactory.const(Service.mk[String, String](Future.value))
        }
      }

    val params =
      Stack.Params.empty + BindingFactory.Dest(unbound) + BindingFactory.BaseDtab(baseDtab)

    val factory = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
      .push(verifyModule)
      .push(BindingFactory.module[String, String])
      .make(params)

    val service = Await.result(factory())
    Await.result(service("foo"))
  }
}

@RunWith(classOf[JUnitRunner])
class DynNameFactoryTest extends FunSuite with MockitoSugar {
  private trait Ctx {
    val cache = mock[ServiceFactoryCache[NameTree[Name.Bound], String,String]]
    val svc = mock[Service[String, String]]
    val (name, namew) = Activity[NameTree[Name.Bound]]()
    val dyn = new DynNameFactory[String, String](name, cache)
  }

  test("DynNameFactory is Busy when name is unresolved")(new Ctx {
    intercept[IllegalStateException] { name.sample() }
    assert(dyn.status === Status.Busy)
  })

  test("DynNameFactory is Closed when name resolution fails")(new Ctx {
    assert(dyn.status === Status.Busy)
    namew.notify(Throw(new Exception("boom")))
    assert(dyn.status === Status.Closed)
  })

  test("DynNameFactory is Closed after closing")(new Ctx {
    assert(dyn.status === Status.Busy)
    Await.ready(dyn.close())
    assert(dyn.status === Status.Closed)
  })

  test("DynNameFactory reflects status of underlying cached service factory")(
    for(status <- Seq(Status.Closed, Status.Busy, Status.Open)) { new Ctx {
      when(cache.status(any[NameTree[Name.Bound]])).thenReturn(status)
      namew.notify(Return(NameTree.Leaf(Name.empty)))
      assert(dyn.status === status)
    }}
  )

  test("queue requests until name is nonpending (ok)")(new Ctx {
    when(cache(any[NameTree[Name.Bound]], any[ClientConnection])).thenReturn(Future.value(svc))

    val f1, f2 = dyn()
    assert(!f1.isDefined)
    assert(!f2.isDefined)

    namew.notify(Return(NameTree.Leaf(Name.empty)))

    assert(f1.poll === Some(Return(svc)))
    assert(f2.poll === Some(Return(svc)))

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

    assert(f1.poll === Some(Throw(Failure(exc, Failure.Naming))))
    assert(f2.poll === Some(Throw(Failure(exc, Failure.Naming))))
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
        assert(cce.getCause === exc)
      case _ => fail()
    }
    assert(f2.poll === None)

    namew.notify(Return(NameTree.Leaf(Name.empty)))
    assert(f2.poll === None)
  })
}

@RunWith(classOf[JUnitRunner])
class NameTreeFactoryTest extends FunSuite {

  test("distributes requests according to weight") {
    val tree =
      NameTree.Union(
        NameTree.Weighted(1D, NameTree.Union(
          NameTree.Weighted(1D, NameTree.Leaf("foo")),
          NameTree.Weighted(1D, NameTree.Leaf("bar")))),
        NameTree.Weighted(1D, NameTree.Leaf("baz")))

    val counts = mutable.HashMap[String, Int]()

    val factoryCache = new ServiceFactoryCache[String, Unit, Unit](
      key => new ServiceFactory[Unit, Unit] {
        def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
          val count = counts.getOrElse(key, 0)
          counts.put(key, count + 1)
          Future.value(null)
        }
        def close(deadline: Time) = Future.Done
      })

    // not the world's greatest test since it depends on the
    // implementation of Drv
    val rng = {
      val ints = Array(0, 0, 0, 1, 1)
      var intIdx = 0

      new Rng {
        def nextDouble() =
          throw new UnsupportedOperationException

        def nextInt(n: Int) = {
          val i = ints(intIdx)
          intIdx += 1
          i
        }

        def nextInt() = ???
        def nextLong(n: Long) = ???
      }
    }

    val factory = NameTreeFactory(
      Path.empty,
      tree,
      factoryCache,
      rng)

    factory.apply(ClientConnection.nil)
    factory.apply(ClientConnection.nil)
    factory.apply(ClientConnection.nil)

    assert(counts("foo") == 1)
    assert(counts("bar") == 1)
    assert(counts("baz") == 1)
  }

  test("is available iff all leaves are available") {
    def isAvailable(tree: NameTree[Status]): Boolean =
      NameTreeFactory(
        Path.empty,
        tree,
        new ServiceFactoryCache[Status, Unit, Unit](
          key => new ServiceFactory[Unit, Unit] {
            def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = Future.value(null)
            def close(deadline: Time) = Future.Done
            override def status = key
          })
        ).isAvailable

    assert(isAvailable(
      NameTree.Union(
        NameTree.Weighted(1D, NameTree.Union(
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open)),
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open)))),
        NameTree.Weighted(1D, NameTree.Leaf(Status.Open)))))

    assert(!isAvailable(
      NameTree.Union(
        NameTree.Weighted(1D, NameTree.Union(
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open)),
          NameTree.Weighted(1D, NameTree.Leaf(Status.Closed)))),
        NameTree.Weighted(1D, NameTree.Leaf(Status.Open)))))

    assert(!isAvailable(
      NameTree.Union(
        NameTree.Weighted(1D, NameTree.Union(
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open)),
          NameTree.Weighted(1D, NameTree.Leaf(Status.Open)))),
        NameTree.Weighted(1D, NameTree.Empty))))
  }
}
