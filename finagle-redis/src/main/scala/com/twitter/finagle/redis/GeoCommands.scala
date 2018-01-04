package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.io.Buf
import com.twitter.io.Buf.Utf8
import com.twitter.util.Future

private[redis] trait GeoCommands {
  self: BaseClient =>

  /**
    * Adds the specified geospatial items (latitude, longitude, name) to the specified `key`.
    *
    * Redis 3.2.0 or later supports this command.
    * For detailed API spec, see:
    *   [[https://redis.io/commands/geoadd]]
    *
    * @return Future of the number of elements added to the sorted set
    */
  def geoAdd(key: Buf, values: GeoElement*): Future[Long] =
    doRequest(GeoAdd(key, values: _*)) {
      case IntegerReply(n) => Future.value(n)
    }

  /**
    * Get valid Geohash strings representing the position of elements.
    *
    * Redis 3.2.0 or later supports this command.
    * For detailed API spec, see:
    *   [[https://redis.io/commands/geohash]]
    *
    * @return Future of an array where each element is the Geohash corresponding
    *         to each member, or `None` if member is missing.
    */
  def geoHash(key: Buf, members: Buf*): Future[Seq[Option[Buf]]] =
    doRequest(GeoHash(key, members: _*)) {
      case EmptyMBulkReply => Future.Nil
      case MBulkReply(hashes) => Future.value(hashes.map {
        case EmptyBulkReply => None
        case BulkReply(hashBuf) => Some(hashBuf)
      })
    }

  /**
    * Get the positions (longitude,latitude) of all the specified members
    * of the geospatial index represented by the sorted set at `key`.
    *
    * Redis 3.2.0 or later supports this command.
    * For detailed API spec, see:
    *   [[https://redis.io/commands/geopos]]
    *
    * @return Future of an array where each element is a tuple representing
    *         longitude and latitude (x,y) of each member, or `None` if member is missing.
    */
  def geoPos(key: Buf, members: Buf*): Future[Seq[Option[(Double, Double)]]] = {
    doRequest(GeoPos(key, members: _*)) {
      case EmptyMBulkReply => Future.Nil
      case MBulkReply(positions) => Future.value(positions.map {
        case MBulkReply(Seq(longitude, latitude)) => (longitude, latitude) match {
          case (BulkReply(lon), BulkReply(lat)) => unapplyCoordinate(lon,lat)
        }
        case _ => None
      })
    }
  }

  private val geoDistResultHandler: PartialFunction[Reply, Future[Option[Double]]] = {
    case EmptyBulkReply => Future.None
    case BulkReply(buf) => Future.value(Utf8.unapply(buf).map(_.toDouble))
  }

  /**
    * Get the distance between two members in the geospatial index represented by the sorted set.
    *
    * Redis 3.2.0 or later supports this command.
    * For detailed API spec, see:
    *   [[https://redis.io/commands/geodist]]
    *
    * @return Future of the distance in the specified unit,
    *         or `None` if one or both the elements are missing.
    */
  def geoDist(key: Buf, member1: Buf, member2: Buf, unit: Option[GeoUnit] = None): Future[Option[Double]] = {
    doRequest(GeoDist(key, member1, member2, unit))(geoDistResultHandler)
  }

  /**
    * Get the members of a sorted set which are within the borders of the area specified
    * with the center location * and the maximum distance from the center (the radius).
    *
    * Redis 3.2.0 or later supports this command.
    * For detailed API spec, see:
    *   [[https://redis.io/commands/georadius]]
    *
    * @return Future of an array where each member represents element.
    *         Each element additionally contains coordinate, distance and geohash if options are specified
    */
  def geoRadius(key: Buf,
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
               ): Future[Seq[Option[GeoRadiusResult]]] = {
    doRequest(GeoRadius(key,
      longitude,
      latitude,
      radius,
      unit,
      withCoord = withCoord,
      withDist = withDist,
      withHash = withHash,
      count = count,
      sort = sort,
      store = store,
      storeDist = storeDist
    ))(handleGeoRadiusResponse)
  }

  /**
    * Get the members of a sorted set which are within the borders of the area specified
    * with the center location of a `member`.
    *
    * Redis 3.2.0 or later supports this command.
    * For detailed API spec, see:
    *   [[https://redis.io/commands/georadiusbymember]]
    *
    * @return Future of an array where each member represents element.
    *         Each element additionally contains coordinate, distance and geohash if options are specified
    *
    */
  def geoRadiusByMember(key: Buf,
                        member: Buf,
                        radius: Double,
                        unit: GeoUnit,
                        withCoord: Boolean = false,
                        withDist: Boolean = false,
                        withHash: Boolean = false,
                        count: Option[Int] = None,
                        sort: Option[Sort] = None,
                        store: Option[Buf] = None,
                        storeDist: Option[Buf] = None
                       ): Future[Seq[Option[GeoRadiusResult]]] = {
    doRequest(GeoRadiusByMember(key,
      member,
      radius,
      unit,
      withCoord = withCoord,
      withDist = withDist,
      withHash = withHash,
      count = count,
      sort = sort,
      store = store,
      storeDist = storeDist
    ))(handleGeoRadiusResponse)
  }

  private val geoRadiusResultParser: PartialFunction[Reply, Option[GeoRadiusResult]] = {
    case EmptyBulkReply => None
    case BulkReply(member) => Some(GeoRadiusResult(member))
    case MBulkReply(attrs) => attrs match {
      case BulkReply(member) :: rest =>
        Some((rest map {
          case IntegerReply(hash) => (r: GeoRadiusResult) => r.copy(hash = Some(hash.toInt))
          case BulkReply(dist) => (r: GeoRadiusResult) => r.copy(dist = Buf.Utf8.unapply(dist).map(_.toDouble))
          case MBulkReply(BulkReply(lon) :: BulkReply(lat) :: Nil) =>
            (r: GeoRadiusResult) => r.copy(coord = unapplyCoordinate(lon,lat))
        }).foldRight(GeoRadiusResult(member))((m, r) => m(r)))
    }
  }

  private val handleGeoRadiusResponse: PartialFunction[Reply, Future[Seq[Option[GeoRadiusResult]]]] = {
    case EmptyBulkReply => Future.Nil
    case EmptyMBulkReply => Future.Nil
    case MBulkReply(replies) => Future.value(replies.map(geoRadiusResultParser))
  }

  private def unapplyCoordinate(longitude: Buf, latitude: Buf): Option[(Double, Double)] = for {
    lon <- Buf.Utf8.unapply(longitude)
    lat <- Buf.Utf8.unapply(latitude)
  } yield (lon.toDouble, lat.toDouble)
}
