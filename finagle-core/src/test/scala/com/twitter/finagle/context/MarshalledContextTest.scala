package com.twitter.finagle.context

import com.twitter.io.Buf
import com.twitter.util.{Return, Throw}

class MarshalledContextTest extends AbstractContextTest {
  val ctx = new MarshalledContext

  val a = new ctx.Key[String]("a.key") {
    def marshal(value: String) = Buf.Utf8(value)
    def tryUnmarshal(buf: Buf) = buf match {
      case Buf.Utf8(value) => Return(value)
    }
  }

  val b = new ctx.Key[Int]("b.key") {
    def marshal(value: Int) = Buf.U32BE(value)
    def tryUnmarshal(buf: Buf) = buf match {
      case Buf.U32BE(value, Buf.Empty) => Return(value)
      case _ => Throw(new IllegalArgumentException)
    }
  }

  test("Translucency: pass through, replace") {
    ctx.let(b, 333) {
      ctx.letUnmarshal(Seq(Buf.Utf8("bleep") -> Buf.Utf8("bloop"))) {
        assert(ctx.contains(b))
        assert(
          ctx.marshal().toMap == Map(
            Buf.Utf8("b.key") -> Buf.U32BE(333),
            Buf.Utf8("bleep") -> Buf.Utf8("bloop")
          )
        )
      }

      ctx.letUnmarshal(Seq(Buf.Utf8("bleep") -> Buf.Utf8("NOPE"))) {
        assert(
          ctx.marshal().toMap == Map(
            Buf.Utf8("b.key") -> Buf.U32BE(333),
            Buf.Utf8("bleep") -> Buf.Utf8("NOPE")
          )
        )
      }
    }
  }

  test("Only marshal the most recent binding for a given key") {

    ctx.letUnmarshal(Seq(a.marshalId -> Buf.Utf8("bloop"))) {
      assert(ctx.get(a) == Some("bloop"))
      assert(ctx.marshal().toMap == Map(a.marshalId -> Buf.Utf8("bloop")))

      ctx.letUnmarshal(Seq(a.marshalId -> Buf.Utf8("ok"))) {
        assert(ctx.get(a) == Some("ok"))
        assert(ctx.marshal().toMap == Map(a.marshalId -> Buf.Utf8("ok")))
      }

      ctx.let(a, "ok") {
        assert(ctx.get(a) == Some("ok"))
        assert(ctx.marshal().toMap == Map(a.marshalId -> Buf.Utf8("ok")))
      }
    }
  }

  test("Translucency: convert ok") {
    val number = 30301952
    ctx.letUnmarshal(Seq(Buf.Utf8("b.key") -> Buf.U32BE(number))) {
      assert(ctx.contains(b))
      assert(ctx.get(b) == Some(number))

      assert(ctx.marshal().toMap == Map(Buf.Utf8("b.key") -> Buf.U32BE(30301952)))
    }
  }

  test("Translucency: convert fail") {
    val number = 30301952
    // This fails because the buffer will be 8 bytes and
    // the unmarshal logic requires strictly 4 bytes.
    ctx.letUnmarshal(Seq(Buf.Utf8("b.key") -> Buf.U64BE(number))) {
      assert(!ctx.contains(b))
      assert(ctx.marshal().toMap == Map(Buf.Utf8("b.key") -> Buf.U64BE(number)))
    }
  }

  test("Unmarshal") {
    ctx.let(Seq(ctx.KeyValuePair(a, "ok"), ctx.KeyValuePair(b, 123), ctx.KeyValuePair(a, "notok"))) {
      val roundTrip = ctx.doUnmarshal(Map.empty, ctx.marshal())

      def checkKey(key: ctx.Key[_]): Unit = {
        roundTrip(key.marshalId) match {
          case t: ctx.Translucent => assert(t.unmarshal(key) == ctx.get(key))
          case other => fail(s"Unexpected structure: $other")
        }
      }

      checkKey(a)
      checkKey(b)

      val marshallRoundtrip = ctx.marshal(roundTrip)
      val marshallDirect = ctx.marshal()

      assert(marshallRoundtrip.iterator.sameElements(marshallDirect.iterator))
    }
  }
}
