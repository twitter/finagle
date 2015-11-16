package com.twitter.finagle.redis.integration

import com.twitter.finagle.redis.ClientError
import com.twitter.finagle.redis.naggati.RedisClientTest
import com.twitter.finagle.redis.tags.{ClientTest, RedisTest}
import com.twitter.finagle.redis.util.StringToChannelBuffer
import com.twitter.util.Await
import scala.collection.{Set => CollectionSet}
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@Ignore
@RunWith(classOf[JUnitRunner])
final class SetClientIntegrationSuite extends RedisClientTest {

  private[this] val oneElemAdded           = 1
  private[this] val oneElemAddErrorMessage = "Could not add one element"
  private[this] val key                    = string2ChanBuf("member")

  test("Correctly add, then pop members of a set", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(bar))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sPop(key)) == Some(bar), "Could not remove bar")

      assert(Await.result(client.sAdd(key, List(baz))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sPop(key)) == Some(baz), "Could not remove baz")
    }
  }

  test("Correctly add, then pop members from a set while counting them", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sCard(key)) == 0)
      assert(Await.result(client.sAdd(key, List(bar))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sCard(key)) == 1)

      assert(Await.result(client.sAdd(key, List(baz))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sCard(key)) == 2)

      Await.result(client.sPop(key))
      assert(Await.result(client.sCard(key)) == 1)
      Await.result(client.sPop(key))
      assert(Await.result(client.sCard(key)) == 0)
    }
  }

  test("Correctly add and pop members from a set, while looking at the set", RedisTest,
    ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(foo))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sIsMember(key, foo)) == true, "Foo was not a member of the set")

      assert(Await.result(client.sIsMember(key, baz)) == false, "Baz was found in the set")
      assert(Await.result(client.sAdd(key, List(baz))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sIsMember(key, baz)) == true, "Baz was not found in the set")
      assert(Await.result(client.sIsMember(key, foo)) == true, "Foo was not a member of the set")

      Await.result(client.sPop(key))
      Await.result(client.sPop(key))
      assert(Await.result(client.sIsMember(key, baz)) == false, "Baz was found in the set")
      assert(Await.result(client.sIsMember(key, foo)) == false, "Foo was found in the set")
    }
  }

  test("Correctly add, examine members of a set, then pop them off and reexamine", RedisTest,
    ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(moo))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sAdd(key, List(boo))) == oneElemAdded, oneElemAddErrorMessage)

      val strings: CollectionSet[String] = Await.result(client.sMembers(key)).map(chanBuf2String(_))
      assert(strings == CollectionSet("moo", "boo"))

      Await.result(client.sPop(key))
      Await.result(client.sPop(key))
      assert(Await.result(client.sMembers(key)) == CollectionSet(), "Collection set was not EMPTY")
    }
  }

  test("Correctly add members to a set, then remove them", RedisTest, ClientTest) {
    withRedisClient { client =>
      assert(Await.result(client.sAdd(key, List(moo))) == oneElemAdded, oneElemAddErrorMessage)
      assert(Await.result(client.sAdd(key, List(boo))) == oneElemAdded, oneElemAddErrorMessage)

      assert(Await.result(client.sRem(key, List(moo))) == 1, "Could not remove one Element")
      assert(Await.result(client.sRem(key, List(boo))) == 1, "Could not remove one Element")
      assert(Await.result(client.sRem(key, List(moo))) == 0, "Removed an element when it should "
        + "not have been possible")
    }
  }

  test("Correctly add member to a set, and return random", RedisTest, ClientTest) {
    withRedisClient { client =>
      val allMembers = Seq(foo, bar)
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
      assert(twoMembers.forall(allMembers.contains(_)) == true, "The two member set did not " +
        "match the original Sequence")

      val setMembers = Await.result(client.sRandMember(key, count = Some(5)))
      assert(setMembers.size == 2)
      assert(setMembers.forall(allMembers.contains(_)) == true, "The set members did not match " +
        "the original Sequence")

      val negMembers = Await.result(client.sRandMember(key, count = Some(-4)))
      assert(negMembers.size == 4, "The set did not handle a negative member")
    }
  }

  test("Correctly perform set intersection variations", RedisTest, ClientTest) {
    withRedisClient { client =>
      val a = StringToChannelBuffer("a")
      val b = StringToChannelBuffer("b")
      val c = StringToChannelBuffer("c")
      val d = StringToChannelBuffer("d")
      val e = StringToChannelBuffer("e")

      val fooMembers = CollectionSet(a, b, c, d)
      Await.result(client.sAdd(foo, fooMembers.toList))
      Await.result(client.sAdd(boo, List(c)))
      Await.result(client.sAdd(baz, List(a, c, e)))
      Await.result(client.sAdd(moo, List(a, b)))

      // Should intersect a single value
      assert(Await.result(client.sInter(Seq(foo, boo, baz))) == CollectionSet(c))

      // Has no intersection
      assert(Await.result(client.sInter(Seq(boo, moo))) == CollectionSet.empty)

      // bar is not a known key
      assert(Await.result(client.sInter(Seq(foo, bar))) == CollectionSet.empty)

      // neither num or bar is a known key
      assert(Await.result(client.sInter(Seq(num, bar))) == CollectionSet.empty)

      val fooInter = Await.result(client.sInter(Seq(foo)))
      // Only one key will give itself as intersection
      assert(fooMembers forall (m => fooInter.contains(m)))

      // At least one non-empty key is required
      intercept[ClientError] {
        Await.result(client.sInter(Seq()))
      }

      intercept[ClientError] {
        Await.result(client.sInter(Seq(StringToChannelBuffer(""))))
      }
    }
  }
}
