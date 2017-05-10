package com.twitter.finagle.loadbalancer

/**
 * The behavior the load balancer should take when none
 * of its nodes have a [[com.twitter.finagle.Status]] of
 * [[com.twitter.finagle.Status.Open Open]].
 *
 * The default behavior is [[WhenNoNodesOpen.PickOne]]
 * and can be customized on a client through [[LoadBalancerFactory.WhenNoNodesOpenParam]]:
 * {{{
 * import com.twitter.finagle.loadbalancer.LoadBalancerFactory.WhenNoNodesOpenParam
 * import com.twitter.finagle.loadbalancer.WhenNoNodesOpen
 * import com.twitter.finagle.Http
 *
 * Http.client
 *   .configured(WhenNoNodesOpenParam(WhenNoNodesOpen.FailFast))
 * }}}
 *
 * @see the [[https://twitter.github.io/finagle/guide/Clients.html#behavior-when-no-nodes-are-available user guide]].
 * @see `WhenNoNodesOpens` for Java friendly API.
 */
sealed trait WhenNoNodesOpen

object WhenNoNodesOpen {

  /**
   * Picks one node, usually at random. This is an optimistic
   * decision that the balancer's view of the nodes may be out-of-date.
   *
   * This is the default load balancer behavior.
   *
   * For a Java friendly API, use `WhenNoNodesOpens.PICK_ONE`.
   *
   * @see [[WhenNoNodesOpen.FailFast]] for a more conservative approach.
   */
  case object PickOne extends WhenNoNodesOpen

  /**
   * Fail the request with a [[NoNodesOpenException]].
   *
   * For a Java friendly API, use `WhenNoNodesOpens.FAIL_FAST`.
   *
   * @see [[WhenNoNodesOpen.PickOne]] for a more optimistic approach.
   */
  case object FailFast extends WhenNoNodesOpen
}
