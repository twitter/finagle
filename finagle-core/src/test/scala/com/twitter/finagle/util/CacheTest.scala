package com.twitter.finagle.util

import com.twitter.util.TimeConversions._
import com.twitter.util._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify}
import org.mockito.Mockito
import org.mockito.Matchers._

@RunWith(classOf[JUnitRunner])
class CacheTest extends FunSuite with MockitoSugar {

  class CacheHelper {
    val timer = new MockTimer
    val evictor = mock[Object => Unit]
    val cache = Mockito.spy(new Cache[Object](5, 5.seconds, timer, Some(evictor)))
    val objects = 0 until 10 map {
      _ => mock[Object]
    }
  }

  test("Cache(5, 5.seconds) should keep at most 5 items") {
    val h = new CacheHelper
    import h._

    objects foreach { cache.put(_) }
    assert(cache.size == 5)
    objects take 5 foreach { obj =>
      verify(evictor)(obj)
    }
  }

  test("Cache(5, 5.seconds) should return objects in LIFO order") {
    val h = new CacheHelper
    import h._

    objects take 5 foreach { cache.put(_) }
    (objects take 5).reverse foreach { x => assert(cache.get() == Some(x)) }
  }

  test("Cache(5, 5.seconds) should return None when empty") {
    val h = new CacheHelper
    import h._

    assert(cache.get() == None)
  }

  test("Cache(5, 5.seconds) should expire items after the TTL") {
    val h = new CacheHelper
    import h._

    tc: TimeControl =>
      cache.put(objects(0))
      assert(timer.tasks.size == 1)
      tc.advance(1.second)
      cache.put(objects(1))
      assert(timer.tasks.size == 1)
      tc.advance(4.seconds)
      timer.tick()
      verify(evictor)(objects(0))
      verify(evictor, times(0))(objects(1))
      assert(timer.tasks.size == 1)
      tc.advance(5.seconds)
      timer.tick()
      verify(evictor)(objects(1))
      assert(timer.tasks.isEmpty)
  }

  test("Cache(5, 5.seconds) should not expire any items if none of them have expired yet") {
    val h = new CacheHelper
    import h._

    tc: TimeControl =>
      cache.put(objects(0))
      assert(timer.tasks.size == 1)
      cache.put(objects(1))
      cache.put(objects(2))
      assert(timer.tasks.size == 1)
      tc.advance(4.seconds)
      timer.tick()
      verify(evictor, times(0))(objects(0))
      verify(evictor, times(0))(objects(1))
      verify(evictor, times(0))(objects(2))
      assert(cache.size == 3)
  }

  test("Cache(5, 5.seconds) should evictAll evicts all items") {
    val h = new CacheHelper
    import h._

    objects take 5 foreach { cache.put(_) }
    assert(cache.size == 5)
    verify(evictor, times(0))(any)
    cache.evictAll()
    objects take 5 foreach { verify(evictor)(_) }
    assert(cache.size == 0)
  }

  test("Cache(5, 5.seconds) should keep a timer only when cache is nonempty") {
    val h = new CacheHelper
    import h._

    assert(timer.tasks.isEmpty)
    cache.put(objects(0))
    assert(timer.tasks.size == 1)
    assert(cache.get() == Some(objects(0)))
    assert(timer.tasks.isEmpty)
  }
}
