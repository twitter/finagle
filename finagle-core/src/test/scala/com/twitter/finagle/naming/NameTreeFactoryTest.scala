package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.factory.ServiceFactoryCache
import com.twitter.finagle.ssl.session.NullSslSessionInfo
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.finagle.util.Rng
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import com.twitter.util.Timer
import java.net.SocketAddress
import org.scalatest.OneInstancePerTest
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class NameTreeFactoryTest
    extends AnyFunSuite
    with OneInstancePerTest
    with ScalaCheckDrivenPropertyChecks {
  val N = 100000

  val counts = mutable.HashMap[String, Int]()
  val factoryCache = new ServiceFactoryCache[String, Unit, Unit](
    key =>
      new ServiceFactory[Unit, Unit] {
        def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = {
          val count = counts.getOrElse(key, 0)
          counts.put(key, count + 1)
          Future.value(null)
        }
        def close(deadline: Time) = Future.Done
      },
    Timer.Nil
  )

  val clientConnection = new ClientConnection {
    val closed = new Promise[Unit]
    def remoteAddress: SocketAddress = new SocketAddress {}
    def localAddress: SocketAddress = new SocketAddress {}
    def close(deadline: Time): Future[Unit] = {
      closed.setDone()
      Future.Done
    }
    def onClose: Future[Unit] = closed
    def sslSessionInfo: SslSessionInfo = NullSslSessionInfo
  }

  test("distributes requests with custom key") {
    val tree =
      NameTree.Union(
        NameTree.Weighted(
          8d,
          NameTree.Union(
            NameTree.Weighted(7d, NameTree.Leaf("a")),
            NameTree.Weighted(1d, NameTree.Leaf("p"))
          )
        ),
        NameTree.Weighted(2d, NameTree.Leaf("s"))
      )

    val rng = Rng(123)
    val factory = NameTreeFactory(Path.empty, tree, factoryCache, rng)

    Contexts.local.let(CustomNameTreeFactoryKey, 23432l) {
      for (_ <- 1 to 5) {
        // Each call to apply will create a new Rng with the CustomNameTreeFactoryKey value as the seed
        factory.apply(ClientConnection.nil)
      }
    }

    // A Rng with a seed is deterministic, so each time one is created with the same seed,
    // we will select the same branch to send traffic to.
    // With 23432l as the seed, "s" will always be selected, regardless of the NameTree.Union weighting
    assert(counts("s") == 5)
  }

  test("distributes requests with custom key while maintaining overall distn") {
    val distn = Map("a" -> .7 * N, "s" -> .2 * N, "p" -> .1 * N)
    val tree =
      NameTree.Union(
        NameTree.Weighted(distn("a"), NameTree.Leaf("a")),
        NameTree.Weighted(distn("s"), NameTree.Leaf("s")),
        NameTree.Weighted(distn("p"), NameTree.Leaf("p"))
      )

    val rng = Rng(123)
    val factory = NameTreeFactory(Path.empty, tree, factoryCache, rng)

    for (_ <- 0 until N) {
      Contexts.local.let(CustomNameTreeFactoryKey, rng.nextLong(Long.MaxValue)) {
        factory()
      }
    }

    counts.foreach {
      case (dest: String, actual: Int) =>
        val expected = distn(dest)
        // This test is deterministic to avoid flakiness - a change to the Rng may change the distribution
        assert((math.abs(expected - actual.toDouble) / expected) < 0.05)
    }
  }

  test("distributes requests according to weight") {
    val tree =
      NameTree.Union(
        NameTree.Weighted(
          1d,
          NameTree.Union(
            NameTree.Weighted(1d, NameTree.Leaf("foo")),
            NameTree.Weighted(1d, NameTree.Leaf("bar"))
          )
        ),
        NameTree.Weighted(1d, NameTree.Leaf("baz"))
      )

    // not the world's greatest test since it depends on the
    // implementation of Drv
    val rng = {
      val ints = Array(0, 0, 0, 1, 1)
      var intIdx = 0

      new Rng {
        def nextDouble() =
          throw new UnsupportedOperationException

        def nextInt(n: Int) = {
          val i = ints(intIdx)
          intIdx += 1
          i
        }

        def nextInt() = ???
        def nextLong(n: Long) = ???
      }
    }

    val factory = NameTreeFactory(Path.empty, tree, factoryCache, rng)

    factory.apply(ClientConnection.nil)
    factory.apply(ClientConnection.nil)
    factory.apply(ClientConnection.nil)

    assert(counts("foo") == 1)
    assert(counts("bar") == 1)
    assert(counts("baz") == 1)
  }

  test("is available iff all leaves are available") {

    assert(
      allLeavesAvailable(
        NameTree.Union(
          NameTree.Weighted(
            1d,
            NameTree.Union(
              NameTree.Weighted(1d, NameTree.Leaf(Status.Open)),
              NameTree.Weighted(1d, NameTree.Leaf(Status.Open))
            )
          ),
          NameTree.Weighted(1d, NameTree.Leaf(Status.Open))
        )
      )
    )

    assert(
      !allLeavesAvailable(
        NameTree.Union(
          NameTree.Weighted(
            1d,
            NameTree.Union(
              NameTree.Weighted(1d, NameTree.Leaf(Status.Open)),
              NameTree.Weighted(1d, NameTree.Leaf(Status.Closed))
            )
          ),
          NameTree.Weighted(1d, NameTree.Leaf(Status.Open))
        )
      )
    )

    assert(
      !allLeavesAvailable(
        NameTree.Union(
          NameTree.Weighted(
            1d,
            NameTree.Union(
              NameTree.Weighted(1d, NameTree.Leaf(Status.Open)),
              NameTree.Weighted(1d, NameTree.Leaf(Status.Open))
            )
          ),
          NameTree.Weighted(1d, NameTree.Empty)
        )
      )
    )
  }

  test("filters Zero-weighted Neg/Empty/Fail out of unions") {
    val oneNonEmpty = NameTree.Union(
      NameTree.Weighted(0.0, NameTree.Neg),
      NameTree.Weighted(0.0, NameTree.Fail),
      NameTree.Weighted(0.0, NameTree.Empty),
      NameTree.Weighted(0.0, NameTree.Leaf("foo"))
    )

    assert(canBuildServiceFromTree(oneNonEmpty))
  }

  test("if only zero-weighted Neg/Empty/Fail in union, then NoBrokersAvailable") {
    val allEmpty = NameTree.Union(
      NameTree.Weighted(0.0, NameTree.Neg),
      NameTree.Weighted(0.0, NameTree.Fail),
      NameTree.Weighted(0.0, NameTree.Empty)
    )

    assert(!canBuildServiceFromTree(allEmpty))
  }

  test("nested all-zero weighted unions filters correctly") {
    val embeddedNonEmpty = NameTree.Union(
      NameTree.Weighted(0.0, NameTree.Neg),
      NameTree.Weighted(0.0, NameTree.Fail),
      NameTree.Weighted(0.0, NameTree.Empty),
      NameTree.Weighted(
        0.0,
        NameTree.Union(
          NameTree.Weighted(0.0, NameTree.Neg),
          NameTree.Weighted(0.0, NameTree.Leaf("foo"))
        ))
    )

    assert(canBuildServiceFromTree(embeddedNonEmpty))
  }

  test("nested not all-zero weighted unions filters correctly") {
    val embeddedNotAllZerosNonEmpty = NameTree.Union(
      NameTree.Weighted(0.0, NameTree.Empty),
      NameTree.Weighted(
        1.0,
        NameTree.Union(
          NameTree.Weighted(0.0, NameTree.Neg),
          NameTree.Weighted(0.0, NameTree.Union(NameTree.Weighted(0.0, NameTree.Leaf("foo"))))
        ))
    )

    assert(canBuildServiceFromTree(embeddedNotAllZerosNonEmpty))
  }

  test("deeply nested not all-zero weighted unions filters correctly") {
    val embeddedNotAllZerosNonEmpty = NameTree.Union(
      NameTree.Weighted(0.0, NameTree.Empty),
      NameTree.Weighted(
        0.0,
        NameTree.Union(
          NameTree.Weighted(0.0, NameTree.Neg),
          NameTree.Weighted(0.0, NameTree.Union(NameTree.Weighted(1.0, NameTree.Leaf("foo"))))
        ))
    )

    assert(canBuildServiceFromTree(embeddedNotAllZerosNonEmpty))
  }

  test("non-zero weighted empty are not filtered") {
    val emptyWeightedNonZero = NameTree.Union(
      NameTree.Weighted(100.0, NameTree.Empty),
      NameTree.Weighted(0.0, NameTree.Leaf("foo"))
    )

    assert(!canBuildServiceFromTree(emptyWeightedNonZero))
  }

  test("non-zero weighted empty are not filtered when retain flag is default") {
    val emptyWeightedNonZero = NameTree.Union(
      NameTree.Weighted(0.5, NameTree.Empty),
      NameTree.Weighted(0.5, NameTree.Leaf(Status.Open))
    )
    assert(!allLeavesAvailable(emptyWeightedNonZero))
  }

  test("non-zero weighted empty are filtered when retain flag set to false") {
    retainUneachableUnionBranches.let(false) {
      val emptyWeightedNonZero = NameTree.Union(
        NameTree.Weighted(0.5, NameTree.Empty),
        NameTree.Weighted(0.5, NameTree.Leaf(Status.Open))
      )
      assert(allLeavesAvailable(emptyWeightedNonZero))
    }
  }

  def canBuildServiceFromTree(tree: NameTree[String]): Boolean = {
    val factoryCache = new ServiceFactoryCache[String, Unit, Unit](
      _ =>
        new ServiceFactory[Unit, Unit] {
          def apply(conn: ClientConnection): Future[Service[Unit, Unit]] =
            Future.value(Service.const(Future.Done))
          def close(deadline: Time) = Future.Done
          override def status = Status.Open
        },
      Timer.Nil
    )

    NameTreeFactory(Path.empty, tree, factoryCache).isAvailable
  }

  def allLeavesAvailable(tree: NameTree[Status]): Boolean =
    NameTreeFactory(
      Path.empty,
      tree,
      new ServiceFactoryCache[Status, Unit, Unit](
        key =>
          new ServiceFactory[Unit, Unit] {
            def apply(conn: ClientConnection): Future[Service[Unit, Unit]] = Future.value(null)
            def close(deadline: Time) = Future.Done
            override def status = key
          },
        Timer.Nil
      )
    ).isAvailable

}
