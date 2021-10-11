package com.twitter.finagle.naming

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.aperture.EagerConnections
import com.twitter.finagle.loadbalancer.aperture.EagerConnectionsType
import com.twitter.finagle.loadbalancer.Balancers
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.param.Stats
import com.twitter.finagle.stack.nilStack
import com.twitter.finagle.stats._
import com.twitter.finagle.stats.exp.Expression
import com.twitter.finagle.stats.exp.ExpressionSchema
import com.twitter.finagle.stats.exp.ExpressionSchemaKey
import com.twitter.finagle.tracing.Annotation
import com.twitter.finagle.tracing.NullTracer
import com.twitter.finagle.tracing.Record
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.tracing.TraceId
import com.twitter.finagle.tracing.Tracer
import com.twitter.util._
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.any
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.spy
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

object BindingFactoryTest {
  object TestNamer {
    var f: Path => Activity[NameTree[Name]] = { _ => Activity.value(NameTree.Neg) }
  }
  class TestNamer extends Namer {
    def lookup(path: Path): Activity[NameTree[Name]] = TestNamer.f(path)
  }
}

case class Factory(i: Int) extends EndpointFactory[String, String] {
  def remake(): Unit = {}
  val address: Address = Address.Failed(new Exception)

  var _total: Int = 0
  def total: Int = _total

  def apply(conn: ClientConnection): Future[Service[String, String]] = {
    _total += 1

    Future.value(new Service[String, String] {
      def apply(unit: String): Future[String] = ???
      override def close(deadline: Time): Future[Unit] = {
        Future.Done
      }
      override def toString = s"Service($i)"
    })
  }

  override def close(deadline: Time): Future[Unit] = {
    Future.Done
  }
}

class BindingFactoryTest extends AnyFunSuite with MockitoSugar with BeforeAndAfter {
  import BindingFactoryTest._

  var saveBase: Dtab = Dtab.empty

  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  before {
    saveBase = Dtab.base
    Dtab.base ++= Dtab.read("""
      /test1010=>/$/inet/1010
    """)
  }

  after {
    Dtab.base = saveBase
    NameInterpreter.global = DefaultInterpreter
    TestNamer.f = { _ => Activity.value(NameTree.Neg) }
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
      Timer.Nil,
      statsReceiver = imsr,
      maxNamerCacheSize = 2,
      maxNameCacheSize = 2
    )

    def newWith(localDtab: Dtab): Service[Unit, Var[Addr]] = {
      Dtab.unwind {
        Dtab.local = localDtab
        await(factory())
      }
    }
  }

  def mkFactory(st: Status) =
    (bound: Name.Bound) =>
      new ServiceFactory[Unit, Var[Addr]] {
        def apply(conn: ClientConnection) =
          Future.value(Service.mk { _ => Future.exception(new Exception("nup")) })
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
      TestNamer.f = { _ => Activity(v) }
      val f =
        Dtab.unwind {
          Dtab.local =
            Dtab.read(s"/foo/bar=>/$$/com.twitter.finagle.naming.BindingFactoryTest$$TestNamer")
          factory()
        }
      tc.advance(5678.microseconds)
      v() = Activity.Ok(NameTree.Leaf(Name.Path(Path.read("/test1010"))))
      await(await(f).close())

      val expected = Map(
        Seq("bind_latency_us") -> Seq(5678.0)
      )

      assert(imsr.stats == expected)
    }
  })

  test("Uses Dtab.base")(new Ctx {
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = await(s1(()))
    assert(v1.sample() == Addr.Bound(Address(1010)))

    s1.close()
  })

  test("Respects Dtab.base changes after service factory creation")(new Ctx {
    // factory is already created here
    Dtab.base ++= Dtab.read("/test1010=>/$/inet/1011")
    val n1 = Dtab.read("/foo/bar=>/test1010")
    val s1 = newWith(n1)
    val v1 = await(s1(()))
    assert(v1.sample() == Addr.Bound(Address(1011)))

    s1.close()
  })

  test("Includes path in NoBrokersAvailableException")(new Ctx {
    val noBrokers = intercept[NoBrokersAvailableException] {
      await(factory())
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
    assert(noBrokers.limitedDtab == Dtab.empty)
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
        await(factory())
      }
    }

    assert(noBrokers.name == "/foo/bar")
    assert(noBrokers.localDtab == localDtab)
    assert(noBrokers.limitedDtab == Dtab.empty)
  }

  test("Traces name information per request")(new Ctx {
    withExpectedTrace(
      {
        val n1 = Dtab.read("/foo/bar=>/test1010")
        val s1 = newWith(n1)
        val v1 = await(s1(()))
        s1.close()
      },
      Seq(
        Annotation.BinaryAnnotation("clnt/namer.path", "/foo/bar"),
        Annotation.BinaryAnnotation("clnt/namer.dtab.base", "/test1010=>/$/inet/1010"),
        Annotation.BinaryAnnotation("clnt/namer.name", "/$/inet/1010")
      )
    )
  })

  test("Caches namers")(new Ctx {
    val n1 = Dtab.read("/foo/bar=>/$/inet/1")
    val n2 = Dtab.read("/foo/bar=>/$/inet/2")
    val n3 = Dtab.read("/foo/bar=>/$/inet/3")
    val n4 = Dtab.read("/foo/bar=>/$/inet/4")

    assert(news == 0)
    await(newWith(n1).close() before newWith(n1).close())
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

    await(newWith(n2).close() before newWith(n3).close())
    assert(news == 4)
    assert(closes == 2)
  })

  test("Caches names")(new Ctx {
    val n1 = Dtab.read("/foo/bar=>/$/inet/1; /bar/baz=>/$/nil")
    val n2 = Dtab.read("/foo/bar=>/$/inet/1")
    val n3 = Dtab.read("/foo/bar=>/$/inet/2")
    val n4 = Dtab.read("/foo/bar=>/$/inet/3")

    assert(news == 0)
    await(newWith(n1).close() before newWith(n1).close())
    assert(news == 1)
    assert(closes == 0)

    await(newWith(n2).close())
    assert(news == 1)
    assert(closes == 0)

    await(newWith(n3).close())
    assert(news == 2)
    assert(closes == 0)

    await(newWith(n4).close())
    assert(news == 3)
    assert(closes == 1)

    await(newWith(n3).close())
    assert(news == 3)
    assert(closes == 1)

    await(newWith(n1).close())
    assert(news == 4)
    assert(closes == 2)

    await(newWith(n2).close())
    assert(news == 4)
    assert(closes == 2)
  })

  test("BindingFactory.Module: filters with bound residual paths") {
    val module = new BindingFactory.Module[Path, Path] {
      protected[this] def boundPathFilter(path: Path) =
        Filter.mk { (in, service) => service(path ++ in) }
    }

    val name = Name.Bound(Var(Addr.Pending), "id", Path.read("/alpha"))

    val end = Stack.leaf(
      Stack.Role("end"),
      ServiceFactory(() => Future.value(Service.mk[Path, Path](Future.value)))
    )

    val params = Stack.Params.empty + BindingFactory.Dest(name)
    val factory = module.toStack(end).make(params)
    val service = await(factory())
    val full = await(service(Path.read("/omega")))
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

    val service = await(factory())
    await(service("foo"))
  }

  test("BindingFactory.Module: augments metrics metadata with path info") {
    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo => /$/inet/1")
    val stats = new InMemoryStatsReceiver()

    val verifyModule =
      new Stack.Module1[Stats, ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the stats were modified properly"

        def make(statsParam: Stats, next: ServiceFactory[String, String]) = {
          val Stats(stats) = statsParam
          stats.counter("foo").incr()
          stats.stat("bar").add(1)
          stats.addGauge("baz") { 0 }
          ServiceFactory.const(Service.mk[String, String](Future.value))
        }
      }

    val params =
      Stack.Params.empty + BindingFactory.Dest(unbound) + BindingFactory.BaseDtab(baseDtab) +
        Stats(stats)

    val factory = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
      .push(verifyModule)
      .push(BindingFactory.module[String, String])
      .make(params)

    val service = await(factory())
    await(service("foo"))
    assert(stats.schemas(Seq("foo")).processPath.get == "/$/inet/1")
    assert(stats.counters(Seq("foo")) == 1)
    assert(stats.schemas(Seq("bar")).processPath.get == "/$/inet/1")
    assert(stats.stats(Seq("bar")) == Seq(1))
    assert(stats.schemas(Seq("baz")).processPath.get == "/$/inet/1")
    assert(stats.gauges(Seq("baz"))() == 0)
  }

  test("BindingFactory.Module: augments metrics expressions with path info") {
    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo => /$/inet/1")
    val stats = new InMemoryStatsReceiver()

    val verifyModule =
      new Stack.Module1[Stats, ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the stats were modified properly"

        def make(statsParam: Stats, next: ServiceFactory[String, String]) = {
          val Stats(stats) = statsParam
          val fooCounter = stats.counter("foo")
          fooCounter.incr()
          stats.stat("bar").add(1)
          stats.addGauge("baz") { 0 }
          val schema = ExpressionSchema("foobar", Expression(fooCounter.metadata))
          stats.registerExpression(schema)
          ServiceFactory.const(Service.mk[String, String](Future.value))
        }
      }

    val params =
      Stack.Params.empty + BindingFactory.Dest(unbound) + BindingFactory.BaseDtab(baseDtab) +
        Stats(stats)

    val factory = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
      .push(verifyModule)
      .push(BindingFactory.module[String, String])
      .make(params)

    val service = await(factory())
    await(service("foo"))
    assert(
      stats.expressions.contains(
        ExpressionSchemaKey("foobar", Map(ExpressionSchema.ProcessPath -> "/$/inet/1"), Nil)))
  }

  test("BindingFactory.Module: DisplayNameBound allows configuring how a bound name is shown") {
    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo => /$/inet/1")
    val stats = new InMemoryStatsReceiver()
    val displayFn = { bound: Name.Bound =>
      bound.id match {
        case path: Path => path.show.reverse
        case _ => fail
      }
    }

    val verifyModule =
      new Stack.Module1[Stats, ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the stats were modified properly"

        def make(statsParam: Stats, next: ServiceFactory[String, String]) = {
          val Stats(stats) = statsParam
          stats.counter("foo").incr()
          stats.stat("bar").add(1)
          stats.addGauge("baz") { 0 }
          ServiceFactory.const(Service.mk[String, String](Future.value))
        }
      }

    val params =
      Stack.Params.empty + BindingFactory.Dest(unbound) + BindingFactory.BaseDtab(baseDtab) + Stats(
        stats) + DisplayBoundName(displayFn)

    val factory = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
      .push(verifyModule)
      .push(BindingFactory.module[String, String])
      .make(params)

    val service = await(factory())
    await(service("foo"))
    assert(stats.schemas(Seq("foo")).processPath.get == "/$/inet/1".reverse)
    assert(stats.counters(Seq("foo")) == 1)
    assert(stats.schemas(Seq("bar")).processPath.get == "/$/inet/1".reverse)
    assert(stats.stats(Seq("bar")) == Seq(1))
    assert(stats.schemas(Seq("baz")).processPath.get == "/$/inet/1".reverse)
    assert(stats.gauges(Seq("baz"))() == 0)
  }

  test(
    "If EagerConnectionsType.ForceWithDtab is set, eager connections are enabled regardless of Dtabs\"") {
    val endpoint = Factory(0)
    val endpointEvent = Activity
      .value(Set(endpoint)).states
      .asInstanceOf[Event[Activity.State[Set[EndpointFactory[_, _]]]]]

    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo=>/$/inet/1")
    val displayFn = { bound: Name.Bound =>
      bound.id match {
        case path: Path => path.show.reverse
        case _ => fail
      }
    }
    val params =
      Stack.Params.empty + BindingFactory.Dest(unbound) + BindingFactory.BaseDtab(baseDtab) +
        DisplayBoundName(displayFn) + LoadBalancerFactory.Endpoints(
        endpointEvent) + EagerConnections(EagerConnectionsType.ForceWithDtab) +
        LoadBalancerFactory.Param(Balancers.aperture())

    val factory: ServiceFactory[String, String] =
      new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
        .push(LoadBalancerFactory.module)
        .push(BindingFactory.module)
        .make(params)

    // We should have already made 2 connections due a bound Dtab and having EagerConnections(true)
    // Because there's no Dtab local, Eager Connections will not be overridden
    assert(endpoint.total == 2)

    // With the flag set, we should still get an extra connection when a new Dtab local is defined
    Dtab.unwind {
      Dtab.local = Dtab.read("/foo=>/$/inet/2")
      await(factory())
      assert(endpoint.total == 4)
    }

    await(factory())
    assert(endpoint.total == 5)

    // With the flag set, we should still get an extra connection when a new Dtab.limited is defined
    Dtab.unwind {
      Dtab.limited = Dtab.read("/foo=>/$/inet/3")
      await(factory())
      assert(endpoint.total == 7)
    }

    await(factory())
    assert(endpoint.total == 8)

  }

  test(
    "If EagerConnectionsType.ForceWithDtab is not set, eager connections with dtab locals are disabled") {
    val endpoint = Factory(0)
    val endpointEvent = Activity
      .value(Set(endpoint)).states
      .asInstanceOf[Event[Activity.State[Set[EndpointFactory[_, _]]]]]

    val unbound = Name.Path(Path.read("/foo"))
    val baseDtab = () => Dtab.base ++ Dtab.read("/foo=>/$/inet/1")
    val displayFn = { bound: Name.Bound =>
      bound.id match {
        case path: Path => path.show.reverse
        case _ => fail
      }
    }
    val params =
      Stack.Params.empty + BindingFactory.Dest(unbound) + BindingFactory.BaseDtab(baseDtab) +
        DisplayBoundName(displayFn) + LoadBalancerFactory.Endpoints(
        endpointEvent) + EagerConnections(EagerConnectionsType.Enable) +
        LoadBalancerFactory.Param(Balancers.aperture())

    val factory: ServiceFactory[String, String] =
      new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
        .push(LoadBalancerFactory.module)
        .push(BindingFactory.module)
        .make(params)

    assert(endpoint.total == 2)

    // Without the flag, we should disable eager connections. Apply will only be called when we call factory()
    Dtab.unwind {
      Dtab.local = Dtab.read("/foo=>/$/inet/2")
      await(factory())
      assert(endpoint.total == 3)
    }

    await(factory())
    assert(endpoint.total == 4)

    Dtab.unwind {
      Dtab.local = Dtab.empty
      Dtab.limited = Dtab.read("/foo=>/$/inet/3")
      await(factory())
      assert(endpoint.total == 5)
    }
  }
}
