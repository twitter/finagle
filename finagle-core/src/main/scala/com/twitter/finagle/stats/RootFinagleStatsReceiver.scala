package com.twitter.finagle.stats

private object RootFinagleStatsReceiver {
  private val RpcSystemLabel = "rpc_system" -> "finagle"
  private val ImplementationLabel = "implementation" -> "finagle"
}

// An implementation detail of the finagle-owned stats receivers. We want to add a
// bunch of dimensional labels and scopes and this lets us do it all at one time
// instead of doing a bunch of composition, and that should hopefully be more efficient.
private[finagle] class RootFinagleStatsReceiver(
  parent: StatsReceiver,
  serviceLabel: String,
  dimensionalScopes: Seq[String])
    extends TranslatingStatsReceiver(parent) {

  import RootFinagleStatsReceiver._

  private[this] val rpcServiceLabels: Map[String, String] =
    Map(RpcSystemLabel, ImplementationLabel, "rpc_service" -> serviceLabel)

  private[this] def translateIdentity(
    identity: MetricBuilder.Identity
  ): MetricBuilder.Identity = {
    // We translate the hierarchicalName as if we'd called `.scope(serviceLabel)` which
    // will be a no-op for empty strings.
    val hierarchicalName =
      if (serviceLabel == "") identity.hierarchicalName
      else serviceLabel +: identity.hierarchicalName

    val dimensionalName = dimensionalScopes ++ identity.dimensionalName
    val labels = rpcServiceLabels ++ identity.labels
    identity.copy(
      hierarchicalName = hierarchicalName,
      dimensionalName = dimensionalName,
      labels = labels,
      hierarchicalOnly = false
    )
  }

  protected def translate(builder: MetricBuilder): MetricBuilder = {
    builder.withIdentity(translateIdentity(builder.identity))
  }

  override private[finagle] def scopeTranslation: NameTranslatingStatsReceiver.Mode =
    NameTranslatingStatsReceiver.FullTranslation

  override def toString: String = s"$self/$serviceLabel"
}
