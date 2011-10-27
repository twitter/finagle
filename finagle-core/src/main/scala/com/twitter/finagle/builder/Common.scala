package com.twitter.finagle.builder

import scala.collection.mutable.HashSet
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Timer

/**
 * Used by builder to throw exceptions if the specification is incomplete.
 * {{{
 * if (!_codec.isDefined)
 *   throw new IncompleteSpecification("No codec was specified")
 * }}}
 */
class IncompleteSpecification(message: String) extends Exception(message)
