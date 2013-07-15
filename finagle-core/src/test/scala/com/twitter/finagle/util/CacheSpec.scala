package com.twitter.finagle.util

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.finagle.MockTimer

class CacheSpec extends SpecificationWithJUnit with Mockito {
  "Cache(5, 5.seconds)" should {
    val timer = new MockTimer
    val evictor = mock[Object => Unit]
    val cache = spy(new Cache[Object](5, 5.seconds, timer, Some(evictor)))
    val objects = 0 until 10 map { _ => mock[Object] }

    "keep at most 5 items" in {
      objects foreach { cache.put(_) }
      cache.size must be_==(5)
      objects take 5 foreach { obj =>
        there was one(evictor)(obj)
      }
    }

    "return objects in LIFO order" in {
      objects take 5 foreach { cache.put(_) }
      (objects take(5)).reverse foreach { cache.get() must beSome(_) }
    }

    "return None when empty" in {
      cache.get() must beNone
    }

    "expire items after the TTL" in Time.withTimeAt(Time.epoch) { tc =>
      cache.put(objects(0))
      timer.tasks must haveSize(1)
      tc.advance(1.second)
      cache.put(objects(1))
      timer.tasks must haveSize(1)
      tc.advance(4.seconds)
      timer.tick()
      there was one(evictor)(objects(0))
      there was no(evictor)(objects(1))
      timer.tasks must haveSize(1)
      tc.advance(5.seconds)
      timer.tick()
      there was one(evictor)(objects(1))
      timer.tasks must beEmpty
    }

    "not expire any items if none of them have expired yet" in Time.withTimeAt(Time.epoch)  { tc =>
      cache.put(objects(0))
      timer.tasks must haveSize(1)
      cache.put(objects(1))
      cache.put(objects(2))
      timer.tasks must haveSize(1)
      tc.advance(4.seconds)
      timer.tick()
      there was no(evictor)(objects(0))
      there was no(evictor)(objects(1))
      there was no(evictor)(objects(2))
      cache.size must be_==(3)
    }

    "evictAll evicts all items" in {
      objects take 5 foreach { cache.put(_) }
      cache.size must be_==(5)
      there was no(evictor)(any)
      cache.evictAll()
      objects take 5 foreach { there was one(evictor)(_) }
      cache.size must be_==(0)
    }

    "keep a timer only when cache is nonempty" in {
      timer.tasks must beEmpty
      cache.put(objects(0))
      timer.tasks must haveSize(1)
      cache.get() must beSome(objects(0))
      timer.tasks must beEmpty
    }
  }
}
