package com.twitter.finagle.client

import com.twitter.finagle.Name
import com.twitter.finagle.naming.BindingFactory.Dest
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stack.Params
import com.twitter.finagle.Stackable
import com.twitter.finagle.client.BackupRequestFilter.Histogram
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param
import com.twitter.finagle.service.ResponseClassifier
import com.twitter.finagle.service.Retries
import com.twitter.finagle.service.RetryBudget
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Showable
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer
import com.twitter.util.tunable.Tunable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import scala.collection.JavaConverters._

private[finagle] object DynamicBackupRequestFilter {
  val role = Stack.Role("DynamicBackupRequestFilter")
  val description = "Send a backup request at a configurable latency, initialized by MethodBuilder"

  private[client] object BRFConfig {
    val Empty = BRFConfig(BackupRequestFilter.Param.Disabled, None, None, None, new PoolKey())

    // In order for the pool key to be unique to the fully formed config, `build` must be called
    // when adding the config to the local for each method.
    def build(brfConfig: BRFConfig): BRFConfig = BRFConfig(
      brfConfig.configParam,
      brfConfig.methodName,
      brfConfig.responseClassifier,
      brfConfig.statsReceiver,
      new PoolKey())
  }

  private[client] final case class BRFConfig(
    configParam: BackupRequestFilter.Param,
    methodName: Option[String],
    responseClassifier: Option[ResponseClassifier],
    statsReceiver: Option[StatsReceiver],
    key: PoolKey)

  private[client] final class PoolKey

  private val brfConfig = new Contexts.local.Key[BRFConfig]()

  def withBackupRequestFilterConfig[T](config: BRFConfig)(f: => T): T = {
    Contexts.local.let(brfConfig, config) { f }
  }

  private def getConfig(): BRFConfig = {
    Contexts.local.getOrElse(brfConfig, () => BRFConfig.Empty)
  }

  def perRequestModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      val role = DynamicBackupRequestFilter.role
      val description = DynamicBackupRequestFilter.description

      override def parameters: Seq[Stack.Param[_]] = {
        Seq(
          implicitly[Stack.Param[param.Timer]],
          implicitly[Stack.Param[Retries.Budget]]
        )
      }

      def make(params: Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val filter = new DynamicBackupRequestFilter[Req, Rep](
          params[Retries.Budget].retryBudget,
          params[Histogram].lowestDiscernibleMsValue,
          params[Histogram].highestTrackableMsValue,
          params[param.Timer].timer,
          params[param.Label].label,
          params[Dest].dest,
          params
        )
        filter.andThen(next)
      }
    }
}

/**
 * A dynamically configurable BackupRequestFilter utilized by MethodBuilder.
 *
 * See [[BackupRequestFilter]] for more information on each parameter.
 */
private[finagle] class DynamicBackupRequestFilter[Req, Rep](
  clientRetryBudget: RetryBudget,
  lowestDiscernibleMsValue: Int,
  highestTrackableMsValue: Int,
  timer: Timer,
  serviceName: String,
  dest: Name,
  params: Params)
    extends SimpleFilter[Req, Rep]
    with Closable {

  import DynamicBackupRequestFilter._

  // Because this filter is dynammic and we need access to the correct filter
  // on each request, we use this pool so we don't need to recompute the filter on
  // every request. Instead, we only compute the filter on the first request and then
  // look up the the filter on supsequent requests.
  private[this] val pool: ConcurrentMap[
    PoolKey,
    BackupRequestFilter[Req, Rep]
  ] = new ConcurrentHashMap()

  def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
    val config = getConfig()
    config match {
      case BRFConfig(
            BackupRequestFilter.Param
              .Configured(maxExtraLoad, sendInterrupts, minSendBackupAfterMs),
            methodName,
            Some(rc),
            Some(sr),
            key) =>
        val filter = registerAndCreateBackupRequestFilter(
          key,
          maxExtraLoad,
          sendInterrupts,
          minSendBackupAfterMs,
          rc,
          sr,
          methodName)
        filter.apply(req, service)
      case _ => service(req)
    }
  }

  private def registerAndCreateBackupRequestFilter(
    key: PoolKey,
    maxExtraLoadTunable: Tunable[Double],
    sendInterrupts: Boolean,
    minSendBackupAfterMs: Int,
    responseClassifier: ResponseClassifier,
    statsReceiver: StatsReceiver,
    methodName: Option[String]
  ): BackupRequestFilter[Req, Rep] = {
    pool.computeIfAbsent(
      key,
      _ => {
        val registryKeyPrefixes: Seq[String] = methodName match {
          case Some(name) => Seq(Showable.show(dest), "methods", name)
          case None => Seq(Showable.show(dest), "methods")
        }
        val value =
          "maxExtraLoad: " + maxExtraLoadTunable().toString + ", sendInterrupts: " + sendInterrupts
        val prefixes = registryKeyPrefixes ++ Seq(BackupRequestFilter.role.name, value)
        ClientRegistry.export(params, prefixes: _*)

        new BackupRequestFilter[Req, Rep](
          maxExtraLoadTunable,
          sendInterrupts,
          minSendBackupAfterMs,
          responseClassifier,
          clientRetryBudget,
          lowestDiscernibleMsValue,
          highestTrackableMsValue,
          statsReceiver.scope("backups"),
          timer,
          serviceName)
      }
    )
  }

  def close(deadline: Time): Future[Unit] =
    Closable.all(pool.values.asScala.toSeq: _*).close(deadline)
}
