package com.twitter.finagle.mux

import com.twitter.finagle.Mux.param.CompressionPreferences
import com.twitter.finagle.{ServiceFactory, Stack, Stackable}

private[finagle] object ExportCompressionUsage {
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[CompressionPreferences, ServiceFactory[Req, Rep]] {
      val role = Stack.Role("ExportCompressionUsage")

      val description = "Exports the Compression parameter to the R* Registry"

      def make(
        config: CompressionPreferences,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = next
    }
}
