package com.twitter.finagle.http.param

import com.twitter.conversions.storage._
import com.twitter.finagle.Stack
import com.twitter.util.StorageUnit

/**
 * automatically send 100-CONTINUE responses to requests which set
 * the 'Expect: 100-Continue' header. See longer note on
 * `com.twitter.finagle.Http.Server#withNoAutomaticContinue`
 */
case class AutomaticContinue(enabled: Boolean)
object AutomaticContinue {
  implicit val automaticContinue: Stack.Param[AutomaticContinue] =
    Stack.Param(AutomaticContinue(true))
}

/**
 * the maximum size of a chunk.
 */
case class MaxChunkSize(size: StorageUnit)
object MaxChunkSize {
  implicit val maxChunkSizeParam: Stack.Param[MaxChunkSize] =
    Stack.Param(MaxChunkSize(8.kilobytes))
}

/**
 * the maximum size of all headers.
 */
case class MaxHeaderSize(size: StorageUnit)
object MaxHeaderSize {
  implicit val maxHeaderSizeParam: Stack.Param[MaxHeaderSize] =
    Stack.Param(MaxHeaderSize(8.kilobytes))
}

/**
 * the maximum size of the initial line.
 */
case class MaxInitialLineSize(size: StorageUnit)
object MaxInitialLineSize {
  implicit val maxInitialLineSizeParam: Stack.Param[MaxInitialLineSize] =
    Stack.Param(MaxInitialLineSize(4.kilobytes))
}

case class MaxRequestSize(size: StorageUnit) {
  require(size < 2.gigabytes, s"MaxRequestSize should be less than 2 Gb, but was $size")
}
object MaxRequestSize {
  implicit val maxRequestSizeParam: Stack.Param[MaxRequestSize] =
    Stack.Param(MaxRequestSize(5.megabytes))
}

case class MaxResponseSize(size: StorageUnit) {
  require(size < 2.gigabytes, s"MaxResponseSize should be less than 2 Gb, but was $size")
}
object MaxResponseSize {
  implicit val maxResponseSizeParam: Stack.Param[MaxResponseSize] =
    Stack.Param(MaxResponseSize(5.megabytes))
}

case class Streaming(enabled: Boolean)
object Streaming {
  implicit val maxResponseSizeParam: Stack.Param[Streaming] =
    Stack.Param(Streaming(enabled = false))
}

case class Decompression(enabled: Boolean)
object Decompression extends {
  implicit val decompressionParam: Stack.Param[Decompression] =
    Stack.Param(Decompression(enabled = true))
}

case class CompressionLevel(level: Int)
object CompressionLevel {
  implicit val compressionLevelParam: Stack.Param[CompressionLevel] =
    Stack.Param(CompressionLevel(-1))
}
