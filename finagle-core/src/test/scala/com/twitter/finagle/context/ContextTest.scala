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

  test("Context.let binds") {
    assert(!ctx.contains(a))
    assert(!ctx.contains(b))

    var ran = 0
    ctx.let(a, "ok") {
      ran += 1
      assert(ctx.contains(a))
      assert(ctx.get(a) === Some("ok"))
      assert(ctx(a) === "ok")
    }
    
    assert(ran === 1)
  }

  test("Context.letEnv binds") {
    val env = ctx.Empty.bound(a, "ok")
    var ran = 0
    ctx.letEnv(env) {
      ran += 1
      assert(ctx.contains(a))
      assert(ctx.get(a) === Some("ok"))
      assert(ctx(a) === "ok")
      assert(!ctx.contains(b))
    }

    assert(ran === 1)
  }

  test("Shadowing binds") {
    var ranInner, ranOuter = 0
    ctx.let(b, 1) {
      ranOuter += 1
      assert(ctx(b) === 1)
      ctx.let(b, 2) {
        ranInner += 1
        assert(ctx(b) === 2)
      }
      assert(ranInner === 1)
      assert(ctx(b) === 1)
    }
    assert(ranOuter === 1)
    assert(ranInner === 1)
  }
  
  test("Clearing") {
    var ranInner, ranOuter = 0
    ctx.let(a, "ok", b, 1) {
      ranOuter += 1
      assert(ctx(b) === 1)
      ctx.letClear(b) {
        ranInner += 1
        assert(ctx.contains(a))
        assert(ctx(a) === "ok")

        assert(!ctx.contains(b))
        assert(!ctx.get(b).isDefined)
        intercept[NoSuchElementException] { ctx(b) }
      }
      assert(ranInner === 1)
    }
    assert(ranOuter === 1)
  }
  
  test("Propagates with future execution") {
    val p = new Promise[Unit]
    
    val f = ctx.let(a, "ok") {
      p before {
        Future.value(ctx(a))
      }
    }

    assert(!f.isDefined)
    p.setDone()
    assert(f.isDefined)
    assert(Await.result(f) === "ok")
  }
}