package com.twitter.finagle.context

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import com.twitter.util.{Await, Future, Promise}

@RunWith(classOf[JUnitRunner])
class ContextTest extends FunSuite with AssertionsForJUnit {
  val ctx = new LocalContext
  val a = new ctx.Key[String]
  val b = new ctx.Key[Int]
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
      assert(ctx.contains(a))
      assert(ctx.get(a) == Some("ok"))
      assert(ctx.getOrElse(a, StrFn) == "ok")
      assert(ctx(a) == "ok")
    }

    assert(ran == 1)
  }

  test("Context.let binds, shadows") {
    val env = ctx.Empty.bound(a, "ok")
    var ran = 0
    ctx.let(env) {
      ran += 1
      assert(ctx.contains(a))
      assert(ctx.get(a) == Some("ok"))
      assert(ctx(a) == "ok")
      assert(ctx.getOrElse(a, StrFn) == "ok")
      assert(!ctx.contains(b))

      val env = ctx.Empty.bound(a, "ok1")
      ctx.let(env) {
        ran *= 3
        assert(ctx.contains(a))
        assert(ctx.get(a) == Some("ok1"))
        assert(ctx.getOrElse(a, StrFn) == "ok1")
        assert(ctx(a) == "ok1")
        assert(!ctx.contains(b))
      }
    }

    assert(ran == 3)
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

  test("letClear individual keys") {
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

  test("Empty Context") {
    val empty = ctx.Empty
    assert(empty.get(a).isEmpty)
    assert(empty.getOrElse(a, StrFn) == DefaultStr)
    intercept[NoSuchElementException] { empty(a) }
    assert(!empty.contains(a))
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
    assert(Await.result(f) == "ok")
  }

  test("letClear all keys, basics") {
    assert(!ctx.contains(a))
    var ran = 0
    ctx.let(a, "0") {
      ran += 1
      assert(ctx(a) == "0")
      ctx.letClear() {
        ran += 1
        assert(!ctx.contains(a))
      }
    }
    assert(ran == 2)
  }

  test("letClear all keys, nested let after clearing") {
    assert(!ctx.contains(a))
    assert(!ctx.contains(b))
    var ran = 0
    ctx.let(a, "a") {
      ran += 1
      assert(ctx(a) == "a")
      ctx.letClear() {
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

  test("letClear all keys, affects current scope") {
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
          ctx.letClear() {
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

}
