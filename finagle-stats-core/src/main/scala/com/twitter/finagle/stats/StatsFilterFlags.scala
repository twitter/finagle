package com.twitter.finagle.stats

import com.twitter.app.GlobalFlag
import java.io.File

/**
 * Denylist of regex, comma-separated. Comma is a reserved character and
 * cannot be used. Used with regexes from statsFilterFile.
 *
 * See https://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilter
    extends GlobalFlag[String](
      "",
      "Comma-separated regexes that indicate which metrics to filter out"
    )

/**
 * Comma-separated denylist of files. Each file may have multiple filters,
 * separated by new lines. Used with regexes from statsFilter.
 *
 * See https://www.scala-lang.org/api/current/#scala.util.matching.Regex
 */
object statsFilterFile
    extends GlobalFlag[Set[File]](
      Set.empty[File],
      "Comma separated files of newline separated regexes that indicate which metrics to filter out"
    )
