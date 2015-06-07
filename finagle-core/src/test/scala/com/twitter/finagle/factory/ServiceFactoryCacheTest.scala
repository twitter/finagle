package com.twitter.finagle.factory

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.tracing.{Annotation, NullTracer, Record, Trace, Tracer}
import com.twitter.util.{Future, Time, Await}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{spy, verify, atLeastOnce}
import org.scalatest.{FunSuite, Tag}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ServiceFactoryCacheTest extends FunSuite with MockitoSugar {
  var tracer: Tracer = _
  var captor: ArgumentCaptor[Record] = _

  override def test(testName: String, testTags: Tag*)(f: => Unit) {
    super.test(testName, testTags:_*) {
      tracer = spy(new NullTracer)
      captor = ArgumentCaptor.forClass(classOf[Record])
      Trace.letTracer(tracer) { f }
      factories = Map.empty
      news = Map.empty
    }
  }

  var factories: Map[Int, Int] = Map.empty
  var news: Map[Int, Int] = Map.empty

  case class SF(i: Int) extends ServiceFactory[String, String] {
    assert(!(factories contains i))
    factories += (i -> 0)
    news += (i -> (1+news.getOrElse(i, 0)))

    def apply(conn: ClientConnection) = Future.value(new Service[String, String] {
      factories = factories + (i -> (factories(i)+1))
      def apply(req: String) = Future.value(i.toString)
      override def close(deadline: Time) = {
        factories += (i -> (factories(i) - 1))
        Future.Done
      }
    })

    def close(deadline: Time) = {
      factories -= i
      Future.Done
    }
  }

  case class exceptingSF(i: Int) extends ServiceFactory[String, String] {
    def apply(conn: ClientConnection) = Future.exception(new Exception("oh no"))
    def close(deadline: Time) = Future.Done
  }


  test("cache, evict") (Time.withCurrentTimeFrozen { tc  =>

    val newFactory: Int => ServiceFactory[String, String] = { i => SF(i) }
    val cache = new ServiceFactoryCache[Int, String, String](newFactory, maxCacheSize=2)

    assert(factories.isEmpty)

    val s1 = Await.result(cache(1, ClientConnection.nil))
    assert(factories === Map(1->1))
    val s2 = Await.result(cache(2, ClientConnection.nil))
    assert(factories === Map(1->1, 2->1))

    val s3 = Await.result(cache(3, ClientConnection.nil))
    assert(factories === Map(1->1, 2->1, 3->1))
    Await.result(s3.close())

    assert(factories === Map(1->1, 2->1))
    Await.result(s2.close())
    tc.advance(1.second)
    assert(factories === Map(1->1, 2->0))
    Await.result(s1.close())
    tc.advance(1.second)
    assert(factories === Map(1->0, 2->0))

    assert(news === Map(1->1, 2->1, 3->1))

    val s3x = Await.result(cache(3, ClientConnection.nil))

    assert(factories === Map(1->0, 3->1))
    assert(news === Map(1->1, 2->1, 3->2))

    val s1x, s1y = Await.result(cache(1, ClientConnection.nil))
    assert(factories === Map(1->2, 3->1))
    assert(news === Map(1->1, 2->1, 3->2))

    val s2x = Await.result(cache(2, ClientConnection.nil))
    assert(factories === Map(1->2, 3->1, 2->1))
    assert(news === Map(1->1, 2->2, 3->2))
  })

  test("traces naming success") (Time.withCurrentTimeFrozen { tc  =>
    val newFactory: Int => ServiceFactory[String, String] = { i => SF(i) }
    val cache = new ServiceFactoryCache[Int, String, String](newFactory, maxCacheSize=2)

    Await.result(cache(1, ClientConnection.nil))
    verify(tracer, atLeastOnce()).record(captor.capture())
    val annotations = captor.getAllValues.asScala collect { case Record(_, _, a, _) => a }

    assert(annotations === Seq(
      Annotation.Message("Interpreter cache miss with key 1"),
      Annotation.Message("Interpreter resolved: 0.seconds") // frozen time
    ))

  })

  test("traces naming failure") (Time.withCurrentTimeFrozen { tc  =>
    val newFactory: Int => ServiceFactory[String, String] = { i => exceptingSF(i) }
    val cache = new ServiceFactoryCache[Int, String, String](newFactory, maxCacheSize=2)

    intercept[Exception] { Await.result(cache(1, ClientConnection.nil)) }
    verify(tracer, atLeastOnce()).record(captor.capture())
    val annotations = captor.getAllValues.asScala collect { case Record(_, _, a, _) => a }

    assert(annotations === Seq(
      Annotation.Message("Interpreter cache miss with key 1"),
      Annotation.Message("Interpreter failed to resolve (java.lang.Exception: oh no). Aborting request.")
    ))
  })
}
