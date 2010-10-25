/** Copyright 2010 Twitter, Inc. */
package com.twitter.finagle.hosebird

case class CachedMessage(message: String, kind: Int)

object CachedMessage {

  // Kind as bitmask
  val KIND_UNKNOWN =       0
  val KIND_STATUS =        1
  val KIND_STATUS_DELETE = 2
  val KIND_LIMIT =         4
  val KIND_SCRUB_GEO =     8
  val KIND_SOCIAL =       16
  val KIND_DM =           32

  // Statuses and friends
  val KIND_STATUS_LIKE =  (KIND_STATUS|KIND_STATUS_DELETE|KIND_LIMIT|KIND_SCRUB_GEO)

  // Scope as bitmask
  val SCOPE_PUBLIC =       1
  val SCOPE_RESTRICTED =   2
  val SCOPE_PRIVATE =      4

  // Public and Restricted can be explicitly selected by userid
  val SCOPE_SELECTABLE =   3

  object Kind extends Enumeration {
    type Kind = Value
    val Unknown =         0
    val StatusUpdate =    1
    val StatusDeletion =  2
    val Limit =           4
    val ScrubGeo =        8
    val Social =         16
    val DirectMessage =  32
  }

}
