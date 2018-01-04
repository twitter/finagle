package com.twitter.finagle.redis.protocol

import com.twitter.io.Buf
import com.twitter.io.Buf.Utf8

case class GeoElement(longitude: Double, latitude: Double, member: Buf)

case class GeoAdd(key: Buf, members: GeoElement*)
  extends StrictKeyCommand {
  override def name: Buf = Command.GEOADD

  override def body: Seq[Buf] = key +: members.flatMap {
    e => Seq(Utf8(e.longitude.toString), Utf8(e.latitude.toString), e.member)
  }
}

case class GeoHash(key: Buf, members: Buf*) extends StrictKeyCommand {
  override def name: Buf = Command.GEOHASH

  override def body: Seq[Buf] = key +: members
}

case class GeoPos(key: Buf, members: Buf*) extends StrictKeyCommand {
  override def name: Buf = Command.GEOPOS

  override def body: Seq[Buf] = key +: members
}

case class GeoDist(key: Buf, member1: Buf, member2: Buf, unit: Option[GeoUnit] = None) extends StrictKeyCommand {
  override def name: Buf = Command.GEODIST

  override def body: Seq[Buf] = Seq(key, member1, member2) ++ unit.map(_.toBuf).toSeq
}

/** Object to declare the order of result set returned from Redis' geospatial command. */
sealed abstract class Sort(val notation: Buf)

object Sort {
  /** Ascending */
  case object Asc extends Sort(Utf8("ASC"))
  /** Descending */
  case object Desc extends Sort(Utf8("DESC"))
}

/**
  * Represents value returned from `GEORADIUS` and `GEORADIUSBYMEMBER` command.
  * This object always contains the name of the member. It also contains coordinate,
  * distance and geohash if command is invoked with options.
  */
case class GeoRadiusResult(member: Buf,
                           coord: Option[(Double, Double)] = None,
                           dist: Option[Double] = None,
                           hash: Option[Int] = None)

sealed trait GeoRadiusBase {
  protected val withCoord: Boolean
  protected val withDist: Boolean
  protected val withHash: Boolean
  protected val count: Option[Int]
  protected val sort: Option[Sort]
  protected val store: Option[Buf]
  protected val storeDist: Option[Buf]

  protected lazy val coordArg: Seq[Buf] = if (withCoord) Seq(Utf8("WITHCOORD")) else Nil
  protected lazy val distArg: Seq[Buf] = if (withDist) Seq(Utf8("WITHDIST")) else Nil
  protected lazy val hashArg: Seq[Buf] = if (withHash) Seq(Utf8("WITHHASH")) else Nil
  protected lazy val countArg: Seq[Buf] = count.filter(_ > 0)
    .map(c => Seq(Utf8("COUNT"), Utf8(c.toString))) getOrElse Nil
  protected lazy val sortArg: Seq[Buf] = sort.map(_.notation).toSeq
  protected lazy val storeArg: Seq[Buf] = store.toSeq
  protected lazy val storeDistArg: Seq[Buf] = storeDist.toSeq

  protected lazy val optionalArgs: Seq[Buf] =
    coordArg ++ distArg ++ hashArg ++ countArg ++ sortArg ++ storeArg ++ storeDistArg
}

case class GeoRadius(key: Buf,
                     longitude: Double,
                     latitude: Double,
                     radius: Double,
                     unit: GeoUnit,
                     withCoord: Boolean = false,
                     withDist: Boolean = false,
                     withHash: Boolean = false,
                     count: Option[Int] = None,
                     sort: Option[Sort] = None,
                     store: Option[Buf] = None,
                     storeDist: Option[Buf] = None
                    )
  extends StrictKeyCommand with GeoRadiusBase {
  override def name: Buf = Command.GEORADIUS

  override def body: Seq[Buf] = {
    Seq(key, Utf8(longitude.toString), Utf8(latitude.toString), Utf8(radius.toString), unit.toBuf) ++ optionalArgs
  }
}

case class GeoRadiusByMember(key: Buf, member: Buf, radius: Double, unit: GeoUnit,
                             withCoord: Boolean = false,
                             withDist: Boolean = false,
                             withHash: Boolean = false,
                             count: Option[Int] = None,
                             sort: Option[Sort] = None,
                             store: Option[Buf] = None,
                             storeDist: Option[Buf] = None
                            ) extends StrictKeyCommand with StrictMemberCommand with GeoRadiusBase {
  override def name: Buf = Command.GEORADIUSBYMEMBER

  override def body: Seq[Buf] = Seq(key, member, Utf8(radius.toString), unit.toBuf) ++ optionalArgs
}

/**
  * Represents unit of geospatial distance. Implementation object of this trait is passed to
  * Redis' geospatial commands.
  */
sealed abstract class GeoUnit(str: String) {
  lazy val toBuf: Buf = Utf8(str)
}

object GeoUnit {

  /** Meter */
  case object Meter extends GeoUnit("m")

  /** Kilometer */
  case object Kilometer extends GeoUnit("km")

  /** Mile */
  case object Mile extends GeoUnit("mi")

  /** Feet */
  case object Feet extends GeoUnit("ft")

}
