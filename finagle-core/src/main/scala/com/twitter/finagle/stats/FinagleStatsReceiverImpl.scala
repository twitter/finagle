package com.twitter.finagle.stats

import com.twitter.finagle.stats.MetricBuilder.IdentityType

// We've extracted the guts out of `FinagleStatsReceiver` into this impl class
// to make it easier to test.
private final class FinagleStatsReceiverImpl(underlyingStatsReceiver: StatsReceiver)
    extends StatsReceiverProxy {

  override private[finagle] def scopeTranslation: NameTranslatingStatsReceiver.Mode =
    NameTranslatingStatsReceiver.FullTranslation

  val self: StatsReceiver = {
    val newLabels = Map(
      "implementation" -> "finagle",
      "rpc_system" -> "finagle"
    )
    TranslatingStatsReceiver
      .translateIdentity(underlyingStatsReceiver) { identity =>
        identity.copy(
          hierarchicalName = "finagle" +: identity.hierarchicalName,
          dimensionalName = "rpc" +: "finagle" +: identity.dimensionalName,
          labels = identity.labels ++ newLabels,
          identityType = identity.identityType.bias(IdentityType.Full)
        )
      }

  }
}
