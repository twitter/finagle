package com.twitter.finagle.hosebird

import CachedMessage._

abstract case class HosebirdMessage(state: State)
class Unknown(state: State) extends HosebirdMessage(state)
class Status(state: State) extends HosebirdMessage(state)
class StatusDelete(state: State) extends HosebirdMessage(state)
class Limit(state: State) extends HosebirdMessage(state)
class ScrubGeo(state: State) extends HosebirdMessage(state)
class Social(state: State) extends HosebirdMessage(state)
class DirectMessage(state: State) extends HosebirdMessage(state)

// Components
case class Geo(lat: Double, long: Double)
case class Place(minLat: Double, minLong: Double, maxLat: Double, maxLong: Double)
case class AnnotationAttribute(label: String, value: String)
case class Annotation(namespace: String, attributes: Seq[AnnotationAttribute])

object State {
  def wrap(state: State): HosebirdMessage = state.kind match {
    case KIND_UNKNOWN =>       new Unknown(state)
    case KIND_STATUS =>        new Status(state)
    case KIND_STATUS_DELETE => new StatusDelete(state)
    case KIND_LIMIT =>         new Limit(state)
    case KIND_SCRUB_GEO =>     new ScrubGeo(state)
    case KIND_SOCIAL =>        new Social(state)
    case KIND_DM =>            new DirectMessage(state)
  }
}

class State {
  def wrapped = State.wrap(this)
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

  // def reset() {
  //   userIdOpt = None
  //   retweetUserIdOpt = None
  //   statusIdOpt = None
  //   limitTrackOpt = None
  //   geoOpt = None
  //   placeOpt = None
  //   sourceOpt = None
  //   inReplyToUserIdOpt = None
  //   idOpt = None
  //   retweetIdOpt = None
  //   createdAtOpt = None
  //   textOpt = None
  //   annotationsOpt = None
  // }
}
