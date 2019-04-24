package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag

/**
 * A global flag that augments a tunable, [[Verbose]], with a comma-separated allowlist for debug
 * metrics. The tunable, [[Verbose]], has a higher priority if defined.
 */
object verbose
    extends GlobalFlag[String](
      "Comma-separated list of *-wildcard expressions to allowlist exporting debug metrics."
    )
