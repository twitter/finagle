package com.twitter.finagle.redis.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.io.Buf
import com.twitter.util.Await

import scala.collection.{Set => CollectionSet}

final class SetClientIntegrationSuite extends RedisClientTest {

  private[this] val oneElemAdded = 1
  private[this] val oneElemAddErrorMessage = "Could not add one element"
  private[this] val key = Buf.Utf8("member")

  val TIMEOUT = 5.seconds

  test("Correctly add, then pop members of a set", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(bufBar))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sPop(key)) == Some(bufBar), "Could not remove bufBar")

      assert(Await.result(client.sAdd(key, List(bufBaz))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sPop(key)) == Some(bufBaz), "Could not remove bufBaz")
    }
  }

  test("Correctly add, then pop members from a set while counting them", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sCard(key)) == 0)
      assert(Await.result(client.sAdd(key, List(bufBar))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sCard(key)) == 1)

      assert(Await.result(client.sAdd(key, List(bufBaz))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sCard(key)) == 2)

      Await.result(client.sPop(key))
      assert(Await.result(client.sCard(key)) == 1)
      Await.result(client.sPop(key))
      assert(Await.result(client.sCard(key)) == 0)
    }
  }

  test(
    "Correctly add and pop members from a set, while looking at the set",
    RedisTest,
    ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(bufFoo))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sIsMember(key, bufFoo)) == true, "Foo was not a member of the set")

      assert(Await.result(client.sIsMember(key, bufBaz)) == false, "Baz was found in the set")
      assert(Await.result(client.sAdd(key, List(bufBaz))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sIsMember(key, bufBaz)) == true, "Baz was not found in the set")
      assert(Await.result(client.sIsMember(key, bufFoo)) == true, "Foo was not a member of the set")

      Await.result(client.sPop(key))
      Await.result(client.sPop(key))
      assert(Await.result(client.sIsMember(key, bufBaz)) == false, "Baz was found in the set")
      assert(Await.result(client.sIsMember(key, bufFoo)) == false, "Foo was found in the set")
    }
  }

  test(
    "Correctly add, examine members of a set, then pop them off and reexamine",
    RedisTest,
    ClientTest
  ) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(bufMoo))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sAdd(key, List(bufBoo))) == oneElemAdded, oneElemAddErrorMessage)

      val strings: CollectionSet[String] = Await.result(client.sMembers(key)).map(b2s)
      assert(strings == CollectionSet("moo", "boo"))

      Await.result(client.sPop(key))
      Await.result(client.sPop(key))
      assert(Await.result(client.sMembers(key)) == CollectionSet(), "Collection set was not EMPTY")
    }
  }

  test("Correctly add members to a set, then remove them", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(bufMoo))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sAdd(key, List(bufBoo))) == oneElemAdded, oneElemAddErrorMessage)

      assert(Await.result(client.sRem(key, List(bufMoo))) == 1, "Could not remove one Element")
      assert(Await.result(client.sRem(key, List(bufBoo))) == 1, "Could not remove one Element")
      assert(
        Await.result(client.sRem(key, List(bufMoo))) == 0,
        "Removed an element when it should "
          + "not have been possible"
      )
    }
  }

  test("Correctly add member to a set, and return random", RedisTest, ClientTest) {
    withRedisClient { client =>
      val allMembers = Seq(bufFoo, bufBar)
      val empty = Await.result(client.sRandMember(key))
      assert(empty.size == 0, "The empty set was not empty!")

      allMembers.foreach(m => {
        assert(Await.result(client.sAdd(key, List(m))) == oneElemAdded, oneElemAddErrorMessage)
      })

      val oneMember = Await.result(client.sRandMember(key))
      assert(oneMember.size == 1, "The one member set had an incorrect number of members")
      assert(allMembers.contains(oneMember.head) == true)

      val twoMembers = Await.result(client.sRandMember(key, count = Some(2)))
      assert(twoMembers.size == 2, "The two member set had an incorrect number of members")
      assert(
        twoMembers.forall(allMembers.contains(_)) == true,
        "The two member set did not " +
          "match the original Sequence"
      )

      val setMembers = Await.result(client.sRandMember(key, count = Some(5)))
      assert(setMembers.size == 2)
      assert(
        setMembers.forall(allMembers.contains(_)) == true,
        "The set members did not match " +
          "the original Sequence"
      )

      val negMembers = Await.result(client.sRandMember(key, count = Some(-4)))
      assert(negMembers.size == 4, "The set did not handle a negative member")
    }
  }

  test("Correctly perform set intersection variations", RedisTest, ClientTest) {
    withRedisClient { client =>
      val a = Buf.Utf8("a")
      val b = Buf.Utf8("b")
      val c = Buf.Utf8("c")
      val d = Buf.Utf8("d")
      val e = Buf.Utf8("e")

      val bufFooMembers = CollectionSet(a, b, c, d)
      Await.result(client.sAdd(bufFoo, bufFooMembers.toList))
      Await.result(client.sAdd(bufBoo, List(c)))
      Await.result(client.sAdd(bufBaz, List(a, c, e)))
      Await.result(client.sAdd(bufMoo, List(a, b)))

      // Should intersect a single value
      assert(Await.result(client.sInter(Seq(bufFoo, bufBoo, bufBaz))) == CollectionSet(c))

      // Has no intersection
      assert(Await.result(client.sInter(Seq(bufBoo, bufMoo))) == CollectionSet.empty)

      // bufBar is not a known key
      assert(Await.result(client.sInter(Seq(bufFoo, bufBar))) == CollectionSet.empty)

      // neither num or bufBar is a known key
      assert(Await.result(client.sInter(Seq(bufNum, bufBar))) == CollectionSet.empty)

      val bufFooInter = Await.result(client.sInter(Seq(bufFoo)))
      // Only one key will give itself as intersection
      assert(bufFooMembers forall (m => bufFooInter.contains(m)))

      // At least one non-empty key is required
      intercept[ClientError] {
        Await.result(client.sInter(Seq()))
      }

      intercept[ClientError] {
        Await.result(client.sInter(Seq(Buf.Empty)))
      }
    }
  }

  ignore("Correctly perform an sscan operation", RedisTest, ClientTest) {
    withRedisClient { client =>
      Await.result(client.sAdd(bufFoo, List(bufBar)))
      Await.result(client.sAdd(bufFoo, List(bufBoo)))
      val res = Await.result(client.sScan(bufFoo, 0L, None, None), TIMEOUT)
      assert(BufToString(res(1)) == "bar")
      val withCount = Await.result(client.sScan(bufFoo, 0L, Some(2L), None), TIMEOUT)
      assert(BufToString(withCount(0)) == "0")
      assert(BufToString(withCount(1)) == "bar")
      assert(BufToString(withCount(2)) == "boo")
      val pattern = Buf.Utf8("b*")
      val withPattern = Await.result(client.sScan(bufFoo, 0L, None, Some(pattern)), TIMEOUT)
      assert(BufToString(withCount(0)) == "0")
      assert(BufToString(withCount(1)) == "bar")
      assert(BufToString(withCount(2)) == "boo")
    }
  }
}
