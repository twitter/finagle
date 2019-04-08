package com.twitter.finagle.client

import com.twitter.finagle.Addr.Metadata
import com.twitter.finagle.client.AddrMetadataExtraction.AddrMetadata
import com.twitter.finagle.{Addr, ServiceFactory, Stack, Stackable}
import com.twitter.util.Duration
import java.util.concurrent.atomic.AtomicReference

/**
 * Latency compensation enables the modification of connection, request, and
 * session acquisition timeouts on a per-endpoint basis.  For instance, if a client
 * has both network-local and trans-continental endpoints, a reasonable latency
 * compensator might add the speed-of-light penalty when communicating with distant
 * endpoints.
 */
object LatencyCompensation {

  object Role extends Stack.Role("LatencyCompensation")

  /**
   * A compensator is a function that takes an arbitrary address metadata map
   * and computes a latency compensation. This compensation can be added to
   * connection and request timeouts. Latency compensation is exposed to the
   * stack via a Stack.Param LatencyCompensation.Compensation.
   */
  case class Compensator(compensator: Addr.Metadata => Duration) {
    def mk(): (Compensator, Stack.Param[Compensator]) =
      (this, Compensator.param)
  }
  object Compensator {
    private[this] val Default = new Function1[Addr.Metadata, Duration] {
      def apply(v1: Metadata): Duration = Duration.Zero
      override def toString: String = "NoCompensation"
    }
    implicit val param =
      Stack.Param(Compensator(Default))
  }

  /**
   * If configured, overrides the default [[Compensator]] used in
   * all un-configured clients. If a caller configures a Compensator,
   * the override value will not be used in favor of the caller-configured
   * value.
   */
  object DefaultOverride {
    private val setting: AtomicReference[Option[Compensator]] =
      new AtomicReference[Option[Compensator]](None)

    // unit-test hook
    private[client] def reset() = { setting.set(None) }

    /**
     * Set an override to use for un-configured clients.
     * @return true if the override was set. false if the override was previously set.
     */
    def set(compensator: Compensator): Boolean =
      setting.compareAndSet(None, Some(compensator))

    def apply(): Option[Compensator] = setting.get()
  }

  /**
   * Set by LatencyCompensation for consumption by other modules.
   * Do not set this Param. Instead configure a Compensator.
   */
  private[finagle] case class Compensation(howlong: Duration) {
    def mk(): (Compensation, Stack.Param[Compensation]) =
      (this, Compensation.param)
  }
  private[finagle] object Compensation {
    implicit val param =
      Stack.Param(Compensation(Duration.Zero))
  }

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module[ServiceFactory[Req, Rep]] {
      val role = Role
      val description = "Sets a latency compensation to be added based on the destination address"
      val parameters = Seq(
        implicitly[Stack.Param[AddrMetadata]],
        implicitly[Stack.Param[Compensator]]
      )
      def make(prms: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {

        // If the caller has configured a Compensator, use that.
        // If there is no configured compensator, look for a default override.
        val Compensator(configured) = prms[Compensator]
        val compensator = DefaultOverride() match {
          case Some(v) if !prms.contains[Compensator] => v.compensator
          case _ => configured
        }

        val AddrMetadata(metadata) = prms[AddrMetadata]
        val compensation = compensator(metadata)
        val compensated = next.make(prms + Compensation(compensation))
        Stack.leaf(this, compensated)
      }
    }

}
