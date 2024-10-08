package com.twitter.finagle.netty4

import com.twitter.app.GlobalFlag

/**
 * Flag for defining the percentage of the desired amount of time spent for I/O in the child event
 * loops. The default value is 50, which means the event loop will try to spend the same amount of
 * time for I/O as for non-I/O tasks.
 *
 * {{
 *  -com.twitter.finagle.netty4.ioRatio=50
 * }}
 *
 */
object ioRatio
    extends GlobalFlag(
      50,
      "Sets the percentage of the desired amount of time spent for I/O in the child event loops. " +
        "The default value is 50, which means the event loop will try to spend the same amount of " +
        "time for I/O as for non-I/O tasks."
    )
