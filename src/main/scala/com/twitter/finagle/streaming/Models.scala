package com.twitter.finagle.streaming

import CachedMessage._

// Components
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
