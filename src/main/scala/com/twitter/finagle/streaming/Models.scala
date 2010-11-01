package com.twitter.finagle.streaming

import CachedMessage._

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
}

case class Geo(lat: Double, long: Double)
case class Place(minLat: Double, minLong: Double, maxLat: Double, maxLong: Double)
case class AnnotationAttribute(label: String, value: String)
case class Annotation(namespace: String, attributes: Seq[AnnotationAttribute])

object Name {
  def forKind(i: Int) = i match {
    case KIND_UNKNOWN =>       "Unknown"
    case KIND_STATUS =>        "Status"
    case KIND_STATUS_DELETE => "StatusDelete"
    case KIND_LIMIT =>         "Limit"
    case KIND_SCRUB_GEO =>     "ScrubGeo"
    case KIND_SOCIAL =>        "Social"
    case KIND_DM =>            "DirectMessage"
  }
}

class State {
  var kind = KIND_UNKNOWN
  var userIdOpt: Option[Long] = None
  var retweetUserIdOpt: Option[Long] = None
  var statusIdOpt: Option[Long] = None
  var limitTrackOpt: Option[Long] = None
  var geoOpt: Option[Geo] = None
  var placeOpt: Option[Place] = None
  var sourceOpt: Option[String] = None
  var inReplyToUserIdOpt: Option[Long] = None
  var idOpt: Option[Long] = None
  var retweetIdOpt: Option[Long] = None
  var createdAtOpt: Option[String] = None
  var textOpt: Option[String] = None
  var annotationsOpt: Option[Seq[Annotation]] = None
}
