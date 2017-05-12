package com.twitter.finagle.tunable

import com.twitter.util.tunable.{NullTunableMap, JsonTunableMapper, ServiceLoadedTunableMap, TunableMap}
import com.twitter.finagle.server.ServerInfo

import java.util.concurrent.ConcurrentHashMap
import java.util.function.{Function => JFunction}
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
private[twitter] object StandardTunableMap {

  private[this] val clientMaps = new ConcurrentHashMap[String, TunableMap]()

  private[this] def composeMap(mutable: TunableMap, serverInfo: ServerInfo) =
    new JFunction[String, TunableMap] {
      def apply(id: String): TunableMap = {
        val json = loadJsonConfig(id, serverInfo)
        TunableMap.of(
          mutable,
          ServiceLoadedTunableMap(id),
          json
        )
      }
    }

  def apply(id: String): TunableMap =
    apply(id, ServerInfo(), TunableMap.newMutable(s"Mutable($id)"))

  // Exposed for testing
  private[tunable] def apply(
    id: String,
    serverInfo: ServerInfo,
    mutable: TunableMap
  ): TunableMap =
    clientMaps.computeIfAbsent(id, composeMap(mutable, serverInfo))

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

    val paths = JsonTunableMapper.pathsByPriority(
      s"com/twitter/tunables/$id/", environmentOpt, instanceIdOpt)

    paths.foldLeft(NullTunableMap: TunableMap) { case (map, path) =>
      map.orElse(JsonTunableMapper().loadJsonTunables(id, path))
    }
  }
}
