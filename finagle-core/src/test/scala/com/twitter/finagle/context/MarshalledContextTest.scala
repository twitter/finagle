package com.twitter.finagle.context

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import com.twitter.util.{Await, Future, Promise, Return, Throw}
import com.twitter.io.Buf

@RunWith(classOf[JUnitRunner])
class MarshalledContextTest extends FunSuite with AssertionsForJUnit {
  val ctx = new MarshalledContext

  val a = new ctx.Key[String]("a.key") {
    def marshal(value: String) = Buf.Utf8(value)
    def tryUnmarshal(buf: Buf) = buf match {
      case Buf.Utf8(value) => Return(value)
      case _ => Throw(new IllegalArgumentException)
    }
  }

  val b = new ctx.Key[Int]("b.key") {
    def marshal(value: Int) = Buf.U32BE(value)
    def tryUnmarshal(buf: Buf) = buf match {
      case Buf.U32BE(value, Buf.Empty) => Return(value)
      case _ => Throw(new IllegalArgumentException)
    }
  }

  test("Only marshal the most recent binding for a given key") {
   var env = ctx.Empty: ctx.Env
   env = env.bound(a, "ok")
   assert(ctx.marshal(env).toMap == Map(
     Buf.Utf8("a.key") -> Buf.Utf8("ok")))

   env = env.bound(b, 123)
   assert(ctx.marshal(env).toMap == Map(
     Buf.Utf8("a.key") -> Buf.Utf8("ok"),
     Buf.Utf8("b.key") -> Buf.U32BE(123)))

   env = env.bound(b, 321)
   assert(ctx.marshal(env).toMap == Map(
     Buf.Utf8("a.key") -> Buf.Utf8("ok"),
     Buf.Utf8("b.key") -> Buf.U32BE(321)))

   assert(ctx.marshal(ctx.OrElse(env, ctx.Empty)).toMap == Map(
     Buf.Utf8("a.key") -> Buf.Utf8("ok"),
     Buf.Utf8("b.key") -> Buf.U32BE(321)))
  }

  test("Only marshal the most recent binding for a given key (OrElse)") {
   val env1 = ctx.Empty.bound(a, "ok").bound(b, 123)
   assert(ctx.marshal(env1).toMap == Map(
     Buf.Utf8("a.key") -> Buf.Utf8("ok"),
     Buf.Utf8("b.key") -> Buf.U32BE(123)))
     
   val env2 = ctx.Empty.bound(b, 321)
   assert(ctx.marshal(ctx.OrElse(env2, env1)).toMap == Map(
     Buf.Utf8("a.key") -> Buf.Utf8("ok"),
     Buf.Utf8("b.key") -> Buf.U32BE(321)))
  }

  test("Translucency: pass through, shadow") {
    var env = ctx.Empty: ctx.Env
    
    env = env.bound(b, 333)
    env = ctx.Translucent(env, Buf.Utf8("bleep"), Buf.Utf8("bloop"))
    assert(env.contains(b))
    assert(ctx.marshal(env).toMap == Map(
      Buf.Utf8("b.key") -> Buf.U32BE(333),
      Buf.Utf8("bleep") -> Buf.Utf8("bloop")))

    env = ctx.Translucent(env, Buf.Utf8("bleep"), Buf.Utf8("NOPE"))
    assert(ctx.marshal(env).toMap == Map(
      Buf.Utf8("b.key") -> Buf.U32BE(333),
      Buf.Utf8("bleep") -> Buf.Utf8("NOPE")))
  }
  

  test("Translucency: convert ok") {
    var env = ctx.Empty: ctx.Env
    
    env = ctx.Translucent(env, Buf.Utf8("b.key"), Buf.U32BE(30301952))
    assert(env.contains(b))
    assert(env(b) == 30301952)
    assert(ctx.marshal(env).toMap == Map(
      Buf.Utf8("b.key") -> Buf.U32BE(30301952)))
  }
  
  test("Translucency: convert fail") {
    var env = ctx.Empty: ctx.Env
    
    env = ctx.Translucent(env, Buf.Utf8("b.key"), Buf.U64BE(30301952))
    assert(!env.contains(b))
    // We still pass the context through unmolested; I'm not sure this
    // is the right thing to do.
    assert(ctx.marshal(env).toMap == Map(
      Buf.Utf8("b.key") -> Buf.U64BE(30301952)))
  }

  test("Unmarshal") {
    var env = ctx.Empty
      .bound(a, "ok")
      .bound(b, 123)
      .bound(a, "notok")

    val env2 = ctx.unmarshal(ctx.marshal(env))

    assert(env(a) == env2(a))
    assert(env(b) == env2(b))

    assert(ctx.marshal(env) == ctx.marshal(env2))
  }

}
