package com.twitter.finagle.toggle

import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Return
import com.twitter.util.Throw
import java.net.URL
import java.security.DigestInputStream
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import scala.collection.JavaConverters._

/**
 * A [[ToggleMap]] that is the composition of multiple underlying
 * [[ToggleMap]] implementations in a specific ordering designed
 * to balance control between the operators/service-owners and the library
 * owners.
 *
 * The ordering is as such:
 *  i. A mutable, in-process [[ToggleMap.Mutable]].
 *  i. The `GlobalFlag`-backed [[ToggleMap]], provided via [[ToggleMap.flags]].
 *  i. The service-owner controlled JSON file-based [[ToggleMap]], provided via [[JsonToggleMap]].
 *  i. The dynamically loaded [[ToggleMap]], provided via [[ServiceLoadedToggleMap.apply]].
 *  i. The library-owner controlled JSON file-based [[ToggleMap]], provided via [[JsonToggleMap]].
 *
 * The expectation is that 1, 2, and 3 give service-owners/operators the tools
 * to operate and test [[Toggle toggles]] while library owners would have control
 * over 4 and 5.
 * Flags and in-memory settings allow for rapid testing and overrides
 * while file-based configs are for static configuration owners have settled on.
 *
 * The JSON file-based configuration works via Java resources and must be
 * placed in specific locations in your classpath's resources:
 * `resources/com/twitter/toggles/configs/`. The file-names take the form
 * `\$libraryName.json` for the library owner's configuration and
 * `\$libraryName-service.json` for the service owner's configuration. As an
 * example, finagle-core would have a
 * `resources/com/twitter/toggles/configs/com.twitter.finagle.json` and service
 * owners can customize toggles via
 * `resources/com/twitter/toggles/configs/com.twitter.finagle-service.json`.
 *
 * The JSON files also support optional environment-specific overrides via
 * files that are examined before the non-environment-specific configs.
 * These environment-specific configs must be placed at
 * `resources/com/twitter/toggles/configs/com.twitter.finagle-\$environment.json`
 * or `resources/com/twitter/toggles/configs/com.twitter.finagle-service-\$environment.json`
 * where the `environment` from [[com.twitter.finagle.server.ServerInfo$.apply]] is used to determine which
 * one to load.
 */
object StandardToggleMap {

  private[this] val log = Logger.get()

  private[this] val libs =
    new ConcurrentHashMap[String, ToggleMap.Mutable]()

  /**
   * Returns all registered [[ToggleMap ToggleMaps]] that have been
   * created by [[apply]], keyed by `libraryName`.
   */
  def registeredLibraries: Map[String, ToggleMap.Mutable] =
    libs.asScala.toMap

  /**
   * Get a [[ToggleMap]] for the given `libraryName`.
   *
   * @note If a given `libraryName` has already been loaded, only a single instance
   * will always be returned for all calls to [[apply]] (even if the `StatsReceiver`
   * differs).
   *
   * @param libraryName if multiple matching service loaded implementations are
   *                    found, this will fail with an `java.lang.IllegalStateException`.
   *                    The names must be in fully-qualified form to avoid
   *                    collisions, e.g. "com.twitter.finagle". Valid characters are
   *                    `A-Z`, `a-z`, `0-9`, `_`, `-`, `.`.
   * @param statsReceiver used to record the outcomes of Toggles. For general
   *                      usage this should not be scoped so that the metrics
   *                      always end up scoped to "toggles/\$libraryName".
   */
  def apply(
    libraryName: String,
    statsReceiver: StatsReceiver,
    duplicateHandling: JsonToggleMap.DuplicateHandling = JsonToggleMap.FailParsingOnDuplicateId
  ): ToggleMap.Mutable =
    apply(
      libraryName,
      statsReceiver,
      ToggleMap.newMutable(s"Mutable($libraryName)"),
      ServerInfo(),
      libs,
      duplicateHandling = duplicateHandling
    )

  /** exposed for testing */
  private[toggle] def apply(
    libraryName: String,
    statsReceiver: StatsReceiver,
    mutable: ToggleMap.Mutable,
    serverInfo: ServerInfo,
    registry: ConcurrentMap[String, ToggleMap.Mutable],
    duplicateHandling: JsonToggleMap.DuplicateHandling
  ): ToggleMap.Mutable = {
    Toggle.validateId(libraryName)

    val svcsJson =
      loadJsonConfig(
        s"$libraryName-service",
        serverInfo,
        JsonToggleMap.DescriptionIgnored,
        JsonToggleMap.FailParsingOnDuplicateId)
    val libsJson = loadJsonConfig(
      libraryName,
      serverInfo,
      JsonToggleMap.DescriptionRequired,
      JsonToggleMap.FailParsingOnDuplicateId)

    val stacked = ToggleMap.of(
      mutable,
      ToggleMap.flags,
      svcsJson,
      ServiceLoadedToggleMap(libraryName),
      libsJson
    )
    val observed = ToggleMap.observed(stacked, statsReceiver.scope("toggles", libraryName))
    val toggleMap = new ToggleMap.Mutable with ToggleMap.Proxy {
      def underlying: ToggleMap = observed
      def put(id: String, fraction: Double): Unit = mutable.put(id, fraction)
      def remove(id: String): Unit = mutable.remove(id)
    }
    val prev = registry.putIfAbsent(libraryName, toggleMap)
    if (prev == null)
      toggleMap
    else
      prev
  }

  private[this] def loadJsonConfig(
    configName: String,
    serverInfo: ServerInfo,
    descriptionMode: JsonToggleMap.DescriptionMode,
    duplicateHandling: JsonToggleMap.DuplicateHandling
  ): ToggleMap = {
    val withoutEnv = loadJsonConfigWithEnv(configName, descriptionMode, duplicateHandling)
    val withEnv = serverInfo.environment match {
      case Some(env) =>
        val e = env.toString.toLowerCase
        loadJsonConfigWithEnv(s"$configName-$e", descriptionMode, duplicateHandling)
      case None =>
        NullToggleMap
    }

    // prefer the environment specific config.
    withEnv.orElse(withoutEnv)
  }

  private[this] def checksum(url: URL): Long = {
    val md = MessageDigest.getInstance("SHA-1")
    val is = new DigestInputStream(url.openStream(), md)
    try {
      val bs = new Array[Byte](128)
      while (is.read(bs, 0, 128) != -1) { /* just consume the input */ }
    } finally {
      is.close()
    }
    val d = md.digest()
    // use the first 8 bytes which should be unique enough for our purposes.
    (d(0) & 0xffL) |
      ((d(1) & 0xffL) << 8) |
      ((d(2) & 0xffL) << 16) |
      ((d(3) & 0xffL) << 24) |
      ((d(4) & 0xffL) << 32) |
      ((d(5) & 0xffL) << 40) |
      ((d(6) & 0xffL) << 48) |
      ((d(7) & 0xffL) << 56)
  }

  // exposed for testing
  private[toggle] def selectResource(configName: String, urls: Seq[URL]): URL = {
    assert(urls.nonEmpty)
    // if the resources are duplicates, that is ok and any can be used.
    // if they are different, we can't be sure which was intended to be used, and fail fast.
    if (urls.size > 1 && urls.map(checksum).distinct.size > 1) {
      throw new IllegalArgumentException(
        s"Multiple differing Toggle config resources found for $configName, ${urls.mkString(", ")}"
      )
    }
    urls.head
  }

  private[finagle] def loadJsonConfigWithEnv(
    configName: String,
    descriptionMode: JsonToggleMap.DescriptionMode,
    duplicateHandling: JsonToggleMap.DuplicateHandling
  ): ToggleMap = {
    val classLoader = getClass.getClassLoader
    val rscPath = s"com/twitter/toggles/configs/$configName.json"
    val rscs = classLoader.getResources(rscPath).asScala.toSeq

    if (rscs.isEmpty) {
      NullToggleMap
    } else {
      val rsc = selectResource(configName, rscs)
      log.debug(s"Toggle config resources found for $configName, using $rsc")
      JsonToggleMap.parse(rsc, descriptionMode, duplicateHandling) match {
        case Throw(t) =>
          throw new IllegalArgumentException(
            s"Failure parsing Toggle config resources for $configName, from $rsc",
            t
          )
        case Return(toggleMap) =>
          toggleMap
      }
    }
  }
}
