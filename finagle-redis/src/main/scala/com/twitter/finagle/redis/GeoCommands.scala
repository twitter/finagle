package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.protocol.commands.GeoCommands._
import com.twitter.io.Buf
import com.twitter.util.Future

/**
 * Implementation of Geo commands for redis, which internally uses sorted sets to work with Geo data
 */
private[redis] trait GeoCommands {
  self: BaseClient =>

  /**
   * Adds member -> score pair `members` to sorted set under the `key`.
   *
   * @note Adding multiple elements only works with redis 2.4 or later.
   * @return The number of elements added to sorted set.
   */
  def geoAdd(key: Buf, members: GeoMember*): Future[Long] = {
    doRequest(GeoAdd(key, members)) {
      case IntegerReply(numMembersAdded) => Future.value(numMembersAdded)
    }
  }

  /**
   * Geo removal, equivalent to sorted set removal as GeoSets are SortedSets underneath
   *
   * @param key     the GeoSet to remove members from
   * @param members the members to remove
   *
   * @return the number of members removed from the set
   */
  def geoRem(key: Buf, members: Seq[Buf]): Future[Long] = {
    doRequest(ZRem(key, members)) {
      case IntegerReply(numMembersRemoved) => Future.value(numMembersRemoved)
    }
  }

  /**
   * Gets the geohash of `members` in sorted set at the `key`.
   *
   * https://redis.io/commands/geohash
   *
   * @param key     the GeoSet to search for the member
   * @param members the GeoSet members whose geo hash we want
   */
  def geoHash(key: Buf, members: Buf*): Future[Seq[Option[String]]] = {
    doRequest(Geohash(key, members)) {
      case EmptyMBulkReply => Future.Nil
      case MBulkReply(hashes) =>
        Future.value(hashes.map {
          case EmptyBulkReply => None
          case BulkReply(Buf.Utf8(memberHashString)) => Some(memberHashString)
        })
    }
  }

  /**
   * Parses result of the georadius operation from redis.
   *
   * See https://redis.io/commands/georadius for output details
   * Briefly, has a return value of:
   *
   * Array reply, specifically:
   *
   * Without any WITH option specified, the command just returns a linear array like ["New York","Milan","Paris"].
   * If WITHCOORD, WITHDIST or WITHHASH options are specified, the command returns an array of arrays, where each sub-array represents a single item.
   */
  private val geoRadiusResultParser: PartialFunction[Reply, Option[GeoRadiusResult]] = {
    case EmptyBulkReply => None
    case BulkReply(Buf.Utf8(member)) => Some(GeoRadiusResult(member))
    case MBulkReply(BulkReply(Buf.Utf8(member)) :: optionalResults) =>
      Some(
        optionalResults
          .map {
            case IntegerReply(hash) =>
              (r: GeoRadiusResult) => r.copy(hash = Some(hash.toInt))
            case BulkReply(Buf.Utf8(dist)) =>
              (r: GeoRadiusResult) => r.copy(dist = Some(dist.toDouble))
            case MBulkReply(BulkReply(Buf.Utf8(lon)) :: BulkReply(Buf.Utf8(lat)) :: Nil) =>
              (r: GeoRadiusResult) => r.copy(coord = Some((lon.toDouble, lat.toDouble)))
          }
          .foldRight(GeoRadiusResult(member))((m, r) => m(r))
      )
    case _ => None
  }

  private val handleGeoRadiusResponse: PartialFunction[Reply, Future[
    Seq[Option[GeoRadiusResult]]
  ]] = {
    case EmptyBulkReply => Future.Nil
    case EmptyMBulkReply => Future.Nil
    case MBulkReply(memberRadiusList) => Future.value(memberRadiusList.map(geoRadiusResultParser))
  }

  /**
   * Get the members of a sorted set which are within the borders of the area specified
   * with the center location * and the maximum distance from the center (the radius).
   *
   * @return Future of an array where each member represents element.
   *         Each element additionally contains coordinate, distance and geohash if options are specified
   */
  def geoRadius(
    key: Buf,
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
    doRequest(
      GeoRadius(
        key,
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
      )
    )(handleGeoRadiusResponse)
  }

  /**
   * Get the members of a sorted set which are within the borders of the area specified
   * with the center location of a `member`.
   *
   * @return Future of an array where each member represents element.
   *         Each element additionally contains coordinate, distance and geohash if options are specified
   *
   */
  def geoRadiusByMember(
    key: Buf,
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
    doRequest(
      GeoRadiusByMember(
        key,
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
      )
    )(handleGeoRadiusResponse)
  }

  /**
   * @param key     the GeoSet to search
   * @param members the members to get lat/lon for
   *
   * @return the lon/lat of the queried member
   */
  def geoPosition(key: Buf, members: Buf*): Future[Seq[Option[(Double, Double)]]] = {
    doRequest(GeoPosition(key, members)) {
      case EmptyMBulkReply => Future.Nil
      case MBulkReply(memberPositions) =>
        Future.value(memberPositions.map {
          case MBulkReply(BulkReply(Buf.Utf8(lon)) :: BulkReply(Buf.Utf8(lat)) :: Nil) =>
            Option((lon.toDouble, lat.toDouble))
          case _ => None
        })
    }
  }

  /**
   * @param key          the GeoSet to search
   * @param fromMember   the first member in the query
   * @param toMember     the second member in the query
   * @param distanceUnit the units used for distance
   *
   * @return distance between two GeoSet members
   */
  def geoDistance(
    key: Buf,
    fromMember: Buf,
    toMember: Buf,
    distanceUnit: GeoUnit = GeoUnit.Meter
  ): Future[Option[Double]] = {
    doRequest(GeoDistance(key, fromMember, toMember, distanceUnit)) {
      case EmptyBulkReply => Future.None
      case BulkReply(Buf.Utf8(buf)) => Future.value(Some(buf.toDouble))
    }
  }
}
