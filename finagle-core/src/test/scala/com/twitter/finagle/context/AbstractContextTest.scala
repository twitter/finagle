package com.twitter.finagle.context

import com.twitter.conversions.DurationOps._
import com.twitter.util.{Await, Future, Promise, Return}
import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractContextTest extends AnyFunSuite with AssertionsForJUnit {
  val ctx: Context
  val a: ctx.Key[String]
  val b: ctx.Key[Int]

  val DefaultStr = "theDefault"
  val StrFn = () => DefaultStr
  val DefaultInt = 999
  val IntFn = () => DefaultInt

  test("Context.let binds") {
    assert(!ctx.contains(a))
    assert(!ctx.contains(b))
    assert(ctx.getOrElse(a, StrFn) == DefaultStr)
    assert(ctx.getOrElse(b, IntFn) == DefaultInt)

    var ran = 0
    ctx.let(a, "ok") {
      ran += 1
      // Test all common access methods
      assert(ctx.contains(a))
      assert(ctx.get(a) == Some("ok"))
      assert(ctx.getOrElse(a, StrFn) == "ok")
      assert(ctx(a) == "ok")
    }

    assert(ran == 1) // Make sure it was only run once
  }

  test("Context.let(Iterable(pair1, pair2, ..)) binds multiple keys") {
    // not set yet
    assert(ctx.get(a) == None)
    assert(ctx.get(b) == None)

    ctx.let(Seq(ctx.KeyValuePair(a, "1"), ctx.KeyValuePair(b, 2))) {
      assert(ctx.get(a) == Some("1"))
      assert(ctx.get(b) == Some(2))
    }

    // Only valid in the let block
    assert(ctx.get(a) == None)
    assert(ctx.get(b) == None)
  }

  test("Shadowing binds") {
    var ranInner, ranOuter = 0
    ctx.let(b, 1) {
      ranOuter += 1
      assert(ctx(b) == 1)
      assert(ctx.getOrElse(b, IntFn) == 1)
      ctx.let(b, 2) {
        ranInner += 1
        assert(ctx(b) == 2)
        assert(ctx.getOrElse(b, IntFn) == 2)
      }
      assert(ranInner == 1)
      assert(ctx(b) == 1)
      assert(ctx.getOrElse(b, IntFn) == 1)
    }
    assert(ranOuter == 1)
    assert(ranInner == 1)
    assert(ctx.getOrElse(b, IntFn) == DefaultInt)
  }

  test("letClear(key) clears key") {
    var ranInner, ranOuter = 0
    ctx.let(a, "ok", b, 1) {
      ranOuter += 1
      assert(ctx(b) == 1)
      ctx.letClear(b) {
        ranInner += 1
        assert(ctx.contains(a))
        assert(ctx(a) == "ok")

        assert(!ctx.contains(b))
        assert(ctx.get(b).isEmpty)
        assert(ctx.getOrElse(b, IntFn) == DefaultInt)
        intercept[NoSuchElementException] { ctx(b) }
      }
      assert(ranInner == 1)
    }
    assert(ranOuter == 1)
  }

  test("letClear(Iterable(key1, key2, ..)) clears multiple keys") {
    // Clear multiple keys at once
    ctx.let(Seq(ctx.KeyValuePair(a, "ok"), ctx.KeyValuePair(b, 0))) {
      assert(ctx.contains(a))
      assert(ctx.contains(b))

      ctx.letClear(Seq(a, b)) {
        assert(!ctx.contains(a))
        assert(!ctx.contains(b))
      }
    }
  }

  test("empty Context") {
    ctx.letClearAll {
      assert(ctx.get(a).isEmpty)
      assert(ctx.getOrElse(a, StrFn) == DefaultStr)
      intercept[NoSuchElementException] { ctx(a) }
      assert(!ctx.contains(a))
    }
  }

  test("Supports multiple keys") {
    ctx.let(a, "ok") {
      ctx.let(b, 1) {
        assert(ctx.get(a) == Some("ok"))
        assert(ctx.get(b) == Some(1))
      }
    }
  }

  test("Propagates with future execution") {
    val p = new Promise[Unit]

    val f = ctx.let(a, "ok") {
      p.before {
        Future.value(ctx(a))
      }
    }

    assert(!f.isDefined)
    p.setDone()
    assert(f.isDefined)
    assert(f.poll == Some(Return("ok")))
  }

  test("letClearAll clears all keys, basics") {
    assert(!ctx.contains(a))
    var ran = 0
    ctx.let(a, "0") {
      ran += 1
      assert(ctx(a) == "0")
      ctx.letClearAll {
        ran += 1
        assert(!ctx.contains(a))
      }
    }
    assert(ran == 2)
  }

  test("letClearAll clears all keys, nested let after clearing") {
    assert(!ctx.contains(a))
    assert(!ctx.contains(b))
    var ran = 0
    ctx.let(a, "a") {
      ran += 1
      assert(ctx(a) == "a")
      ctx.letClearAll {
        ran += 1
        assert(!ctx.contains(a))
        ctx.let(b, 2) {
          ran += 1
          assert(!ctx.contains(a))
          assert(ctx(b) == 2)
        }
      }
      assert(ctx(a) == "a")
      assert(!ctx.contains(b))
    }
    assert(ran == 3)
  }

  test("letClearAll clears all keys, affects current scope") {
    assert(!ctx.contains(a))
    assert(!ctx.contains(b))
    var ran = 0
    ctx.let(a, "ok") {
      ran += 1
      assert(ctx(a) == "ok")
      ctx.let(b, 1) {
        ran += 1
        assert(ctx(b) == 1)
        ctx.let(a, "ok2") {
          ran += 1
          assert(ctx(a) == "ok2")
          ctx.letClearAll {
            ran += 1
            assert(!ctx.contains(a))
            assert(!ctx.contains(b))
          }
        }
        assert(ctx(a) == "ok")
        assert(ctx(b) == 1)
      }
    }
    assert(ran == 4)
  }

  test("Recursive binds don't result in StackOverflowError") {
    def loop(i: Int): Future[Unit] = {
      if (i <= 0) {
        assert(ctx.get(b) == None) // Forces evaluation of the whole context
        Future.value(())
      } else
        ctx.let(a, "value") {
          // We propagate the current context for use in the `before` block, but trampoline it
          // by using a Future so we don't get a SOE from trying to build the computation itself.
          Future.value(()).before(loop(i - 1))
        }
    }

    val f = loop(20 * 1024).liftToTry // should be enough to cause a SOE, if its going to happen
    val r = Await.result(f, 10.seconds)
    assert(r.isReturn)
  }
}
