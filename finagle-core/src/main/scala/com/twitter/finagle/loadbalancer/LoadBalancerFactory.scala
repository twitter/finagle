package com.twitter.finagle.loadbalancer

import com.twitter.app.GlobalFlag
import com.twitter.finagle._
import com.twitter.finagle.stats._
import com.twitter.util.{Activity, Var}
import java.net.SocketAddress
import java.util.logging.{Level, Logger}
import scala.collection.mutable

object perHostStats extends GlobalFlag(false, "enable/default per-host stats.\n" +
  "\tWhen enabled,the configured stats receiver will be used,\n" +
  "\tor the loaded stats receiver if none given.\n" +
  "\tWhen disabled, the configured stats receiver will be used,\n" +
  "\tor the NullStatsReceiver if none given.")

object LoadBalancerFactory {
  val role = Stack.Role("LoadBalancer")

  /**
   * A class eligible for configuring a client's load balancer probation setting.
   */
  case class EnableProbation(enable: Boolean)

  implicit object EnableProbation extends Stack.Param[EnableProbation] {
    val default = EnableProbation(false)
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] per host
   * [[com.twitter.finagle.stats.StatsReceiver]]. If the per-host StatsReceiver is
   * not null, the load balancer will broadcast stats to it (scoped with the
   * "host:port" pair) for each host in the destination. For clients with a
   * large host sets in their destination, this can cause unmanageable
   * memory pressure.
   */
  case class HostStats(hostStatsReceiver: StatsReceiver) {
    def mk(): (HostStats, Stack.Param[HostStats]) =
      (this, HostStats.param)
  }

  object HostStats {
    implicit val param = Stack.Param(HostStats(NullStatsReceiver))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a collection
   * of addrs to load balance.
   */
  case class Dest(va: Var[Addr]) {
    def mk(): (Dest, Stack.Param[Dest]) =
      (this, Dest.param)
  }

  object Dest {
    implicit val param = Stack.Param(Dest(Var.value(Addr.Neg)))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]] with a label
   * for use in error messages.
   */
  case class ErrorLabel(label: String) {
    def mk(): (ErrorLabel, Stack.Param[ErrorLabel]) =
      (this, ErrorLabel.param)
  }

  object ErrorLabel {
    implicit val param = Stack.Param(ErrorLabel("unknown"))
  }

  /**
   * A class eligible for configuring a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   */
  case class Param(loadBalancerFactory: LoadBalancerFactory) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  object Param {
    implicit val param = Stack.Param(Param(DefaultBalancerFactory))
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.loadbalancer.LoadBalancerFactory]].
   * The module creates a new `ServiceFactory` based on the module above it for each `Addr`
   * in `LoadBalancerFactory.Dest`. Incoming requests are balanced using the load balancer
   * defined by the `LoadBalancerFactory.Param` parameter.
   */
  private[finagle] def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new BalancerStackModule[Req, Rep] {
      val description = "Balance requests across multiple endpoints"
      val parameters = Seq(
        implicitly[Stack.Param[ErrorLabel]],
        implicitly[Stack.Param[Dest]],
        implicitly[Stack.Param[LoadBalancerFactory.Param]],
        implicitly[Stack.Param[LoadBalancerFactory.HostStats]],
        implicitly[Stack.Param[param.Stats]],
        implicitly[Stack.Param[param.Logger]],
        implicitly[Stack.Param[param.Monitor]],
        implicitly[Stack.Param[param.Reporter]])
    }
}

/**
 * A thin interface around a Balancer's contructor that allows Finagle to pass in
 * context from the stack to the balancers at construction time.
 *
 * @see [[Balancers]] for a collection of available balancers.
 */
abstract class LoadBalancerFactory {
  /**
   * Returns a new balancer which is represented by a [[com.twitter.finagle.ServiceFactory]].
   *
   * @param endpoints The load balancer's collection is usually populated concurrently.
   * So the interface to build a balancer is wrapped in an [[com.twitter.util.Activity]]
   * which allows us to observe this process for changes. Note, the given weights are
   * maintained relative to load assignments not QPS. For example, a weight of 2.0 means
   * that the endpoint will receive twice the amount of load w.r.t the other endpoints
   * in the balancer.
   *
   * @param statsReceiver The StatsReceiver which balancers report stats to. See
   * [[com.twitter.finagle.loadbalancer.Balancer]] to see which stats are exported
   * across implementations.
   *
   * @param emptyException The exception returned when a balancer's collection is empty.
   */
  def newBalancer[Req, Rep](
    endpoints: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep]
}

/**
 * We expose the ability to configure balancers per-process via flags. However,
 * this is generally not a good idea as Finagle processes usually contain many clients.
 * This will likely go away in the future or be no-op and, therfore, should not be
 * depended on. Instead, configure your balancers via the `configured` method on
 * clients:
 *
 * {{
 *    val balancer = Balancers.newAperture(...)
 *    Protocol.configured(LoadBalancerFactory.Param(balancer))
 * }}
 */
object defaultBalancer extends GlobalFlag("heap", "Default load balancer")

package exp {
  object loadMetric extends GlobalFlag("leastReq",
    "Metric used to measure load across endpoints (leastReq | ewma)")
}

object apertureParams extends GlobalFlag("5.seconds:0.5:2.0:1",
  "Aperture parameters: smoothWin:lowLoad:highLoad:minAperture")

object DefaultBalancerFactory extends LoadBalancerFactory {
  import com.twitter.finagle.util.parsers._
  import com.twitter.conversions.time._

  private val log = Logger.getLogger(getClass.getName)

  /**
   * Instantiate the aperture balancer with parameters read from the
   * `apertureParams` flag. If they are malformed, use the defaults.
   */
  private def aperture(): LoadBalancerFactory =
    apertureParams() match {
      case list(duration(smoothWin), double(lowLoad), double(highLoad), int(minAperture)) =>
        log.info("Instantiating aperture balancer with params "+
          s"smoothWin=$smoothWin; lowLoad=$lowLoad; "+
          s"highLoad=$highLoad; minAperture=$minAperture")
        require(lowLoad > 0, "lowLoad <= 0")
        require(lowLoad <= highLoad, "highLoad < lowLoad")
        require(smoothWin > 0.seconds, "smoothWin <= 0.seconds")
        require(minAperture > 0, "minAperture <= 0")
        Balancers.aperture(
          smoothWin=smoothWin,
          lowLoad=lowLoad,
          highLoad=highLoad,
          minAperture=minAperture)
      case bad =>
        log.warning(s"Bad aperture parameters $bad; using system defaults.")
        Balancers.aperture()
    }

  /**
   * Instantiate P2C with a load metric read from the `exp.loadMetric` flag.
   */
  private def p2c(): LoadBalancerFactory =
    exp.loadMetric() match {
      case "ewma" =>
        log.info("Using load metric ewma")
        Balancers.p2cPeakEwma()
      case _ =>
        log.info("Using load metric leastReq")
        Balancers.p2c()
    }

  private val underlying =
    defaultBalancer() match {
      case "heap" => Balancers.heap()
      case "choice" => p2c()
      case "aperture" => aperture()
      case x =>
        log.warning(s"""Invalid load balancer ${x}, using "heap" balancer.""")
        Balancers.heap()
    }

  def newBalancer[Req, Rep](
    endpoints: Activity[Set[(ServiceFactory[Req, Rep], Double)]],
    statsReceiver: StatsReceiver,
    emptyException: NoBrokersAvailableException
  ): ServiceFactory[Req, Rep] = {
    underlying.newBalancer(endpoints, statsReceiver, emptyException)
  }
}
