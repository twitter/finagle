package com.twitter.finagle.loadbalancer.distributor

import com.twitter.finagle.addr.WeightedAddress
import com.twitter.finagle.Addr
import com.twitter.finagle.Address
import com.twitter.finagle.Dtab
import com.twitter.finagle.Status
import com.twitter.logging.Logger
import com.twitter.util.Activity
import com.twitter.util.Event
import com.twitter.util.Var
import scala.collection.mutable.Builder
import scala.collection.immutable
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * A collection of methods and traits used for managing Address lifecycles
 * within the loadbalancer.
 */
private[finagle] object AddrLifecycle {

  val log = Logger.get(getClass.getName)

  /**
   * Transforms a [[Var]] of bound Addresses to an [[Activity]] of bound Addresses
   * @note The Addr.Bound metadata is stripped out in this method, the metadata can
   *       be found in [[AddrMetadataExtraction]]
   */
  def varAddrToActivity(dest: Var[Addr], label: String): Activity[Set[Address]] =
    Activity(dest.map {
      case Addr.Bound(set, _) => Activity.Ok(set)
      case Addr.Neg =>
        log.info(
          s"$label: name resolution is negative (limited dtab: ${Dtab.limited} local dtab: ${Dtab.local})")
        Activity.Ok(Set.empty[Address])
      case Addr.Failed(e) =>
        log.info(
          s"$label: name resolution failed  (limited dtab: ${Dtab.limited} local dtab: ${Dtab.local})",
          e)
        Activity.Failed(e)
      case Addr.Pending =>
        log.debug(s"$label: name resolution is pending")
        Activity.Pending
    })

  /**
   * Folds and accumulates over an [[Activity]] based event `stream` while biasing
   * for success by suppressing intermediate failures.
   *
   * Once we have seen data (via Activity.Ok), ignore Activity.Pending/Failed state
   * changes and keep using stale data to prevent discarding possibly valid data
   * if the updating activity is having transient failures.
   */
  def safelyScanLeft[T, U](
    init: U,
    stream: Event[Activity.State[T]]
  )(
    f: (U, T) => U
  ): Event[Activity.State[U]] = {
    val initState: Activity.State[U] = Activity.Ok(init)
    stream.foldLeft(initState) {
      case (Activity.Pending, Activity.Ok(update)) => Activity.Ok(f(init, update))
      case (Activity.Failed(_), Activity.Ok(update)) => Activity.Ok(f(init, update))
      case (Activity.Ok(state), Activity.Ok(update)) => Activity.Ok(f(state, update))
      case (stale @ Activity.Ok(state), Activity.Failed(_)) if init != state => stale
      case (stale @ Activity.Ok(state), Activity.Pending) if init != state => stale
      case (_, failed @ Activity.Failed(_)) => failed
      case (_, Activity.Pending) => Activity.Pending
    }
  }

  /**
   * This interface handles operations when partitioning a Set of elements into a Map.
   */
  trait DiffOps[U, Partition] {

    /** Removes/cleans up an existing partition */
    def remove(partition: Partition): Unit

    /** Constructs a new partition from the an addition set of elements */
    def add(current: Set[U]): Partition

    /** Updates the existing partition with a set of elements */
    def update(current: Set[U], partition: Partition): Partition
  }

  /**
   * Transform the current element Set to a new Partition Map which updates the previous
   * Partition Map by provided operations.
   *
   * @param accumulated Previous Partition Map
   * @param current     Current Set of elements
   * @param getKeys      A discriminator function for current Set to get keys
   * @param diffOps     [[DiffOps]] to handle the diff between the previous partition map and
   *                    newly transformed partition map from the current Set. This function
   *                    usually handles transactions during creating, updating, and removing.
   */
  def updatePartitionMap[Key, Partition, U](
    accumulated: Map[Key, Partition],
    current: Set[U],
    getKeys: U => Seq[Key],
    diffOps: DiffOps[U, Partition]
  ): Map[Key, Partition] = {
    val grouped = groupBy(current, getKeys)
    val removals = accumulated.keySet &~ grouped.keySet
    val additions = grouped.keySet &~ accumulated.keySet
    val updates = grouped.keySet & accumulated.keySet
    removals.foreach { key => diffOps.remove(accumulated(key)) }
    val added = additions.map { key => key -> diffOps.add(grouped(key)) }.toMap
    val updated = updates.map { key => key -> diffOps.update(grouped(key), accumulated(key)) }.toMap
    added ++ updated
  }

  /**
   * A modified version of scala collection's groupBy function. `f` is a multi-mapping function
   * and this achieves many to many mapping.
   */
  def groupBy[U, Key](
    coll: Set[U],
    f: U => Seq[Key]
  ): immutable.Map[Key, Set[U]] = {
    val m = mutable.Map.empty[Key, Builder[U, Set[U]]]
    for (elem <- coll) {
      val keys = f(elem)
      for (k <- keys) {
        val bldr = m.getOrElseUpdate(k, Set.newBuilder)
        bldr += elem
      }
    }
    val b = immutable.Map.newBuilder[Key, Set[U]]
    for ((k, v) <- m)
      b += ((k, v.result))
    b.result
  }

  def removeStaleAddresses[Req, Rep](
    merged: Map[Address, AddressedFactory[Req, Rep]],
    addresses: Set[Address],
    eagerEviction: Boolean
  ): Map[Address, AddressedFactory[Req, Rep]] = {
    // Remove stale cache entries. When `eagerEviction` is false cache
    // entries are only removed in subsequent stream updates.
    val removed: Set[Address] = merged.keySet -- addresses.map {
      case WeightedAddress(addr, _) => addr
    }
    removed.foldLeft(merged) {
      case (cache, addr) =>
        cache.get(addr) match {
          case Some(AddressedFactory(f, _)) if eagerEviction || f.status != Status.Open =>
            try f.close()
            catch {
              case NonFatal(t) => log.warning(t, s"unable to close endpoint $addr")
            }
            cache - addr
          case _ => cache
        }
    }
  }
}
