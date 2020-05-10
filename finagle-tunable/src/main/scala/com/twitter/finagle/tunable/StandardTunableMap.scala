package com.twitter.finagle.tunable

import com.twitter.util.tunable.{
  JsonTunableMapper,
  NullTunableMap,
  ServiceLoadedTunableMap,
  TunableMap
}
import com.twitter.finagle.server.ServerInfo

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{BiFunction, Function => JFunction}
import scala.collection.JavaConverters._

/**
 * Object used for getting the [[TunableMap]] for a given `id`. This [[TunableMap]] is composed
 * from 3 sources, in order of priority:
 *
 *  i. A mutable, in-process [[TunableMap.Mutable]].
 *  i. The dynamically loaded [[TunableMap]], provided via [[ServiceLoadedTunableMap.apply]].
 *  i. The JSON file-based [[TunableMap]], provided via [[JsonTunableMapper.loadJsonTunables]].
 *
 *  The JSON file-based [[TunableMap]] is a composition of file-based per-instance and
 *  per-environment [[TunableMap]]s. [[TunableMap]]s are composed in the following priority order:
 *
 *  i.  Environment and instance-specific
 *  i.  Environment-specific for all instances
 *  i.  Instance-specific
 *  i.  All instances
 *
 *  For more information, see
 *  [[https://twitter.github.io/finagle/guide/Configuration.html#tunables]].
 */
object StandardTunableMap {

  private[this] val clientMaps = new ConcurrentHashMap[String, TunableMap]()

  private[this] val composeMap = (mutable: TunableMap, serverInfo: ServerInfo, id: String) => {
    val json = loadJsonConfig(id, serverInfo)
    TunableMap.of(
      mutable,
      ServiceLoadedTunableMap(id),
      json
    )
  }

  def apply(id: String): TunableMap =
    apply(id, ServerInfo(), TunableMap.newMutable(s"Mutable($id)"))

  // Exposed for testing
  private[tunable] def apply(id: String, serverInfo: ServerInfo, mutable: TunableMap): TunableMap =
    clientMaps.computeIfAbsent(
      id,
      new JFunction[String, TunableMap] {
        def apply(ID: String): TunableMap = composeMap(mutable, serverInfo, id)
      })

  /**
   * Re-compose [[TunableMap]]s after ServerInfo initialized, re-subscribe
   * [[com.twitter.util.tunable.ServiceLoadedTunableMap]]s to ConfigBus.
   *
   * @note this should be called after ServerInfo.initialized().
   */
  private[twitter] def reloadAll(): Unit = {
    ServiceLoadedTunableMap.reloadAll()
    clientMaps.keys().asScala.toSeq.foreach { id =>
      clientMaps.computeIfPresent(
        id,
        new BiFunction[String, TunableMap, TunableMap] {
          def apply(id: String, curr: TunableMap): TunableMap = {
            val mutable = collectFirstOrElse(
              TunableMap.components(curr),
              TunableMap.newMutable(s"Mutable($id)")
            )
            composeMap(mutable, ServerInfo(), id)
          }
        }
      )
    }
  }

  private[this] def collectFirstOrElse(
    elements: Seq[TunableMap],
    default: TunableMap.Mutable
  ): TunableMap.Mutable = {
    elements
      .collectFirst {
        case mutable: TunableMap.Mutable => mutable
      }.getOrElse(default)
  }

  /**
   * Returns all registered [[TunableMap TunableMaps]] that have been
   * created by [[apply]], keyed by `id`.
   */
  def registeredIds: Map[String, TunableMap] =
    clientMaps.asScala.toMap

  /**
   * Load `TunableMap`s from JSON configuration files and compose them in the path order from
   * `JsonTunableMapper.pathsByPriority`.
   */
  private[tunable] def loadJsonConfig(id: String, serverInfo: ServerInfo): TunableMap = {
    val environmentOpt = serverInfo.environment
    val instanceIdOpt = serverInfo.instanceId

    val paths =
      JsonTunableMapper.pathsByPriority(s"com/twitter/tunables/$id/", environmentOpt, instanceIdOpt)

    paths.foldLeft(NullTunableMap: TunableMap) {
      case (map, path) =>
        map.orElse(JsonTunableMapper().loadJsonTunables(id, path))
    }
  }
}
