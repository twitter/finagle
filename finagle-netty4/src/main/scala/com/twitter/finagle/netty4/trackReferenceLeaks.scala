package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag

/**
 * Enable reference leak tracking in netty and export a counter at finagle/netty4/reference_leaks.
 *
 * @note By default samples 1% of buffers but this rate can increased via the
 *       "io.netty.leakDetection.level" env variable.
 *
 * @see https://netty.io/wiki/reference-counted-objects.html#wiki-h3-11
 */
private object trackReferenceLeaks
    extends GlobalFlag[Boolean](
      false,
      "Enable reference leak tracking in Netty and export a counter at finagle/netty4/reference_leaks"
    )
