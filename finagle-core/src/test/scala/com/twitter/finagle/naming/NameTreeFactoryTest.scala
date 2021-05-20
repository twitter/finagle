package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.finagle.factory.ServiceFactoryCache
import com.twitter.finagle.util.Rng
import com.twitter.util.{Future, Time, Timer}
import scala.collection.mutable
import org.scalatest.funsuite.AnyFunSuite

class NameTreeFactoryTest extends AnyFunSuite {
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
    def isAvailable(tree: NameTree[Status]): Boolean =
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

    assert(
      isAvailable(
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
      !isAvailable(
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
      !isAvailable(
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
}
