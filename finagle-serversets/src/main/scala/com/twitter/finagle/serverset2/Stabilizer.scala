package com.twitter.finagle.serverset2

import com.twitter.finagle.partitioning.zk.ZkMetadata
import com.twitter.finagle.{Addr, Address}
import com.twitter.util._
import java.net.InetSocketAddress

/**
 * The Stabilizer attempts to dampen address changes for the observers so that the they
 * are not affected by transient or flapping states from the external system which
 * supplies the source addresses. It does so in two important ways:
 *
 * 1. Failure stabilization: In case of transient failures (flapping), we suppress failed resolution
 * states when we have an existing good state until we get new information or sufficient time has
 * passed. The previously known good state is returned indefinitely until a new update which puts
 * the responsibility of managing stale data on the callers.
 *
 * 2. Update stabilization: In scenarios where the source is volatile or churning the
 * underlying addresses, the stabilizer buffers (and batches) consecutive runs of
 * bound addresses so that bursty changes to individual addresses are not propagated immediately.
 */
private object Stabilizer {

  /**
   * The state object used to coalesce updates.
   *
   * @param result represents the [[Addr]] which will be published to the event.
   *
   * @param buffer represents the buffer of [[Addr.Bound]] addresses
   * which merges consecutive bound updates and eventually is flushed to `result`.
   *
   * @param last the most recently observed [[Addr.Bound]].
   */
  private case class State(result: Addr, buffer: Addr.Bound, last: Addr.Bound)

  private val EmptyBound: Addr.Bound = Addr.Bound(Set(), Addr.Metadata.empty)
  private val InitState = State(Addr.Pending, EmptyBound, EmptyBound)

  private def coalesce(addrOrEpoch: Event[Either[Addr, Unit]]): Event[Addr] = {
    addrOrEpoch
      .foldLeft(InitState) {
        // new addr bound – merge it with our buffer.
        case (State(result, buffer, _), Left(newBound: Addr.Bound)) =>
          // if `result` is non-bound flush `newBound` immediately (buffer has been reset already)
          val newResult = result match {
            case _: Addr.Bound => result
            case _ => newBound
          }
          // We propagate the metadata from `newBound` and replace the
          // addresses with the merged set.
          val newBuffer = newBound.copy(addrs = merge(buffer.addrs, newBound.addrs))
          State(newResult, newBuffer, newBound)

        // non-bound address
        case (state, Left(nonBound)) =>
          (state.result, nonBound) match {
            // failure/pending: propagate stale state if we have a previous bound.
            case (_: Addr.Bound, Addr.Failed(_) | Addr.Pending) => state
            // This guarantees that `result` is never set to an [[Addr.Failed]].
            case (_, Addr.Failed(_)) => state.copy(result = Addr.Neg)
            case (_, Addr.Pending) => state.copy(result = Addr.Pending)
            // All other non-bound state gets propagated immediately. The state is also reset here.
            case (_, _) => State(nonBound, EmptyBound, EmptyBound)
          }

        // epoch turned – promote buffer.
        case (State(_: Addr.Bound, buffer, last), Right(())) => State(buffer, last, last)
        case (state, Right(())) => state
      }.map {
        case State(result, _, _) => result
      }
  }

  /**
   * Merge `next` with `prev` taking into account shard ids and weights, preferring
   * `next` over `prev` when encountering duplicates. If shardIds are present eliminate
   * duplicates using that, otherwise eliminate duplicates based on raw addresses. ShardId
   * based de-duping is extremely important for observers working against partitioned clusters.
   */
  private def merge(prev: Set[Address], next: Set[Address]): Set[Address] = {
    var shards: Set[Int] = Set.empty
    var inets: Set[InetSocketAddress] = Set.empty

    // populate `shards` and `inets` with the data from `next`
    val nextIter = next.iterator
    while (nextIter.hasNext) {
      nextIter.next() match {
        case Address.Inet(inet, md) =>
          inets = inets + inet
          ZkMetadata.fromAddrMetadata(md) match {
            case Some(ZkMetadata(Some(shardId), _)) =>
              shards = shards + shardId
            case _ => // nop
          }
        case _ => // nop
      }
    }

    // the subset of `prev` to merge with `next`
    val filteredPrev: Set[Address] = prev.filter {
      case Address.Inet(inet, md) =>
        ZkMetadata.fromAddrMetadata(md) match {
          case Some(ZkMetadata(Some(shardId), _)) => !shards.contains(shardId)
          case _ => !inets.contains(inet)
        }
      case _ => true
    }

    filteredPrev ++ next
  }

  def apply(va: Var[Addr], epoch: Epoch): Var[Addr] = {
    Var.async[Addr](Addr.Pending) { updatableAddr =>
      val addrOrEpoch: Event[Either[Addr, Unit]] =
        if (epoch.period != Duration.Zero) {
          va.changes.select(epoch.event)
        } else {
          // This is a special case for integration testing, we want to
          // prevent the update dampening behavior by triggering
          // epochs manually.
          new Event[Either[Addr, Unit]] {
            def register(s: Witness[Either[Addr, Unit]]): Closable = {
              va.changes.respond { addr =>
                s.notify(Left(addr))
                s.notify(Right(()))
                s.notify(Right(()))
              }
            }
          }
        }

      coalesce(addrOrEpoch).register(Witness(updatableAddr))
    }
  }
}
