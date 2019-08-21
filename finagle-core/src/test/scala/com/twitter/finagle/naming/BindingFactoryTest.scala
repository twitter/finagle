package com.twitter.finagle.naming

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats._
import com.twitter.finagle.tracing.{Annotation, NullTracer, Record, Trace, TraceId, Tracer}
import com.twitter.util._
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.{atLeastOnce, spy, verify, when}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.JavaConverters._

object BindingFactoryTest {
  object TestNamer {
    var f: Path => Activity[NameTree[Name]] = { _ =>
      Activity.value(NameTree.Neg)
    }
  }
  class TestNamer extends Namer {
    def lookup(path: Path): Activity[NameTree[Name]] = TestNamer.f(path)
  }
}

class BindingFactoryTest extends FunSuite with MockitoSugar with BeforeAndAfter {
  import BindingFactoryTest._

  var saveBase: Dtab = Dtab.empty

  before {
    saveBase = Dtab.base
    Dtab.base ++= Dtab.read("""
      /test1010=>/$/inet/1010
    """)
  }

  after {
    Dtab.base = saveBase
    NameInterpreter.global = DefaultInterpreter
    TestNamer.f = { _ =>
      Activity.value(NameTree.Neg)
    }
  }

  trait Ctx {
    def withExpectedTrace(f: => Unit, expected: Seq[Annotation]): Unit = {
      val tracer: Tracer = spy(new NullTracer)
      when(tracer.isActivelyTracing(any[TraceId])).thenReturn(true)
      val captor: ArgumentCaptor[Record] = ArgumentCaptor.forClass(classOf[Record])
      Trace.letTracer(tracer) { f }
      verify(tracer, atLeastOnce()).record(captor.capture())
      val annotations = captor.getAllValues.asScala collect { case Record(_, _, a, _) => a }
      assert(expected == annotations)
    }

    val imsr = new InMemoryStatsReceiver

    val path = Path.read("/foo/bar")

    var news = 0
    var closes = 0

    val tcOpt: Option[TimeControl] = None

    lazy val newFactory: Name.Bound => ServiceFactory[Unit, Var[Addr]] =
      bound =>
        new ServiceFactory[Unit, Var[Addr]] {
          news += 1
          def apply(conn: ClientConnection) = {
            tcOpt.foreach(_.advance(1234.microseconds))
            Future.value(Service.mk { _ =>
              Future.value(bound.addr)
            })
          }

          def close(deadline: Time) = {
            closes += 1
            Future.Done
          }
      }

    lazy val factory = new BindingFactory(
      path,
      newFactory,
      Timer.Nil,
      statsReceiver = imsr,
      maxNamerCacheSize = 2,
      maxNameCacheSize = 2
    )

    def newWith(localDtab: Dtab): Service[Unit, Var[Addr]] = {
      Dtab.unwind {
        Dtab.local = localDtab
        Await.result(factory())
      }
    }
  }

  def mkFactory(st: Status) =
    (bound: Name.Bound) =>
      new ServiceFactory[Unit, Var[Addr]] {
        def apply(conn: ClientConnection) =
          Future.value(Service.mk { _ =>
            Future.exception(new Exception("nup"))
          })
        def close(deadline: Time) = Future.Done
        override def status = st
    }

  test("BindingFactory reflects status of underlying cached service factory")(
    for (status <- Seq(Status.Busy, Status.Open, Status.Closed)) {
      new Ctx {
        override lazy val newFactory = mkFactory(status)

        // no binding yet
        assert(factory.status == Status.Closed)

        Dtab.unwind {
          Dtab.local = Dtab.read("/foo/bar=>/test1010")
          assert(factory.status == status)
        }
      }
    }
  )

  test("stats")(Time.withCurrentTimeFrozen { tc =>
    new Ctx {
      override val tcOpt = Some(tc)

      val v = Var[Activity.State[NameTree[Name]]](Activity.Pending)
      TestNamer.f = { _ =>
        Activity(v)
      }
      val f =
        Dtab.unwind {
          Dtab.local =
            Dtab.read(s"/foo/bar=>/$$/com.twitter.finagle.naming.BindingFactoryTest$$TestNamer")
          factory()
        }
      tc.advance(5678.microseconds)
      v() = Activity.Ok(NameTree.Leaf(Name.Path(Path.read("/test1010"))))
      Await.result(Await.result(f).close())

      val expected = Map(
        Seq("bind_latency_us") -> Seq(5678.0)
      )

      assert(imsr.stats == expected)
    }
  })

  test("Uses Dtab.base")(new Ctx {
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = Await.result(s1(()))
    assert(v1.sample() == Addr.Bound(Address(1010)))

    s1.close()
  })

  test("Respects Dtab.base changes after service factory creation")(new Ctx {
    // factory is already created here
    Dtab.base ++= Dtab.read("/test1010=>/$/inet/1011")
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = Await.result(s1(()))
    assert(v1.sample() == Addr.Bound(Address(1011)))

    s1.close()
  })

  test("Includes path in NoBrokersAvailableException")(new Ctx {
    val noBrokers = intercept[NoBrokersAvailableException] {
      Await.result(factory())
    }

    assert(noBrokers.name == "/foo/bar")
    assert(noBrokers.localDtab == Dtab.empty)
  })

  test("Includes path and Dtab.local in NoBrokersAvailableException from name resolution")(new Ctx {
    val localDtab = Dtab.read("/baz=>/quux")

    val noBrokers = intercept[NoBrokersAvailableException] {
      newWith(localDtab)
    }

    assert(noBrokers.name == "/foo/bar")
    assert(noBrokers.localDtab == localDtab)
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
      },
      Timer.Nil
    )

    val noBrokers = intercept[NoBrokersAvailableException] {
      Dtab.unwind {
        Dtab.local = localDtab
        Await.result(factory())
      }
    }

    assert(noBrokers.name == "/foo/bar")
    assert(noBrokers.localDtab == localDtab)
  }

  test("Trace on success")(new Ctx {
    withExpectedTrace(
      {
        val n1 = Dtab.read("/foo/bar=>/test1010")
        val s1 = newWith(n1)
        val v1 = Await.result(s1(()))
        s1.close()
      },
      Seq(
        Annotation.BinaryAnnotation("namer.path", "/foo/bar"),
        Annotation.BinaryAnnotation("namer.dtab.base", "/test1010=>/$/inet/1010"),
        Annotation.Message("namer.success"),
        Annotation.BinaryAnnotation("namer.tree", "/$/inet/1010"),
        Annotation.BinaryAnnotation("namer.name", "/$/inet/1010")
      )
    )
  })

  test("Trace on exception")(new Ctx {
    withExpectedTrace(
      {
        val exc = new RuntimeException

        NameInterpreter.global = new NameInterpreter {
          override def bind(dtab: Dtab, path: Path) = Activity.exception(exc)
        }

        assert(intercept[Failure](Await.result(factory())).isFlagged(FailureFlags.Naming))
      },
      Seq(
        Annotation.BinaryAnnotation("namer.path", "/foo/bar"),
        Annotation.BinaryAnnotation("namer.dtab.base", "/test1010=>/$/inet/1010"),
        Annotation.BinaryAnnotation("namer.failure", "java.lang.RuntimeException")
      )
    )
  })

  test("Trace on negative resolution")(new Ctx {
    withExpectedTrace(
      {
        intercept[NoBrokersAvailableException] {
          Await.result(factory())
        }
      },
      Seq(
        Annotation.BinaryAnnotation("namer.path", "/foo/bar"),
        Annotation.BinaryAnnotation("namer.dtab.base", "/test1010=>/$/inet/1010"),
        Annotation.Message("namer.success"),
        Annotation.BinaryAnnotation("namer.tree", "~")
      )
    )
  })

  test("Trace on service creation failure")(new Ctx {
    withExpectedTrace(
      {
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
          Timer.Nil
        )

        intercept[NoBrokersAvailableException] {
          Dtab.unwind {
            Dtab.local = localDtab
            Await.result(f())
          }
        }
      },
      Seq(
        Annotation.BinaryAnnotation("namer.path", "/foo/bar"),
        Annotation.BinaryAnnotation("namer.dtab.base", "/test1010=>/$/inet/1010"),
        Annotation.Message("namer.success"),
        Annotation.BinaryAnnotation("namer.tree", "/$/inet/1010"),
        Annotation.BinaryAnnotation("namer.name", "/$/inet/1010")
      )
    )
  })

  test("Caches namers")(new Ctx {
    val n1 = Dtab.read("/foo/bar=>/$/inet/1")
    val n2 = Dtab.read("/foo/bar=>/$/inet/2")
    val n3 = Dtab.read("/foo/bar=>/$/inet/3")
    val n4 = Dtab.read("/foo/bar=>/$/inet/4")

    assert(news == 0)
    Await.result(newWith(n1).close() before newWith(n1).close())
    assert(news == 1)
    assert(closes == 0)

    val s2 = newWith(n2)
    assert(news == 2)
    assert(closes == 0)

    // This should evict n1
    val s3 = newWith(n3)
    assert(news == 3)
    assert(closes == 1)

    // n2, n3 are outstanding, so additional requests
    // should hit the one-shot path.
    val s1 = newWith(n1)
    assert(news == 4)
    assert(closes == 1)
    // Closing this should close the factory immediately.
    s1.close()
    assert(closes == 2)

    Await.result(newWith(n2).close() before newWith(n3).close())
    assert(news == 4)
    assert(closes == 2)
  })

  test("Caches names")(new Ctx {
    val n1 = Dtab.read("/foo/bar=>/$/inet/1; /bar/baz=>/$/nil")
    val n2 = Dtab.read("/foo/bar=>/$/inet/1")
    val n3 = Dtab.read("/foo/bar=>/$/inet/2")
    val n4 = Dtab.read("/foo/bar=>/$/inet/3")

    assert(news == 0)
    Await.result(newWith(n1).close() before newWith(n1).close())
    assert(news == 1)
    assert(closes == 0)

    Await.result(newWith(n2).close())
    assert(news == 1)
    assert(closes == 0)

    Await.result(newWith(n3).close())
    assert(news == 2)
    assert(closes == 0)

    Await.result(newWith(n4).close())
    assert(news == 3)
    assert(closes == 1)

    Await.result(newWith(n3).close())
    assert(news == 3)
    assert(closes == 1)

    Await.result(newWith(n1).close())
    assert(news == 4)
    assert(closes == 2)

    Await.result(newWith(n2).close())
    assert(news == 4)
    assert(closes == 2)
  })

  test("BindingFactory.Module: filters with bound residual paths") {
    val module = new BindingFactory.Module[Path, Path] {
      protected[this] def boundPathFilter(path: Path) =
        Filter.mk { (in, service) =>
          service(path ++ in)
        }
    }

    val name = Name.Bound(Var(Addr.Pending), "id", Path.read("/alpha"))

    val end = Stack.leaf(
      Stack.Role("end"),
      ServiceFactory(() => Future.value(Service.mk[Path, Path](Future.value)))
    )

    val params = Stack.Params.empty + BindingFactory.Dest(name)
    val factory = module.toStack(end).make(params)
    val service = Await.result(factory())
    val full = Await.result(service(Path.read("/omega")))
    assert(full == Path.read("/alpha/omega"))
  }

  test("BindingFactory.Module: replaces Dest for bound name") {
    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo => /$/inet/1")

    val verifyModule =
      new Stack.Module1[BindingFactory.Dest, ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the dest was set properly"

        def make(dest: BindingFactory.Dest, next: ServiceFactory[String, String]) = {
          dest match {
            case BindingFactory.Dest(bound: Name.Bound) =>
              assert(bound.id == Path.read("/$/inet/1"))
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
