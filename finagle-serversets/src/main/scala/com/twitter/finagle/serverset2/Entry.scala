package com.twitter.finagle.serverset2

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import com.twitter.util.Memoize

/**
 * Represents one logical serverset2 entry.
 */
sealed trait Entry

/**
 * Endpoints encode a destination announced via serversets.
 *
 * @param names The endpoint name. Null describes a default service
 * endpoint.
 *
 * @param host The host of the endpoint (or null if unset).
 *
 * @param port The port of the endpoint (or Int.MinValue if unset).
 *
 * @param shard The shard id of the endpoint (or Int.MinValue if unset).
 *
 * @param status The endpoint's status.
 *
 * @param memberId The endpoint's member id,
 * used as a foreign key for endpoints.
 *
 * @param metadata The metadata associated with the endpoint.
 */
case class Endpoint(
  names: Array[String],
  host: String,
  port: Int,
  shard: Int,
  status: Endpoint.Status.Value,
  memberId: String,
  metadata: Map[String, String])
    extends Entry {

  override def equals(that: Any) =
    that match {
      case that: Endpoint =>
        java.util.Arrays
          .equals(this.names.asInstanceOf[Array[Object]], that.names.asInstanceOf[Array[Object]]) &&
          this.host == that.host &&
          this.port == that.port &&
          this.shard == that.shard &&
          this.status == that.status &&
          this.memberId == that.memberId &&
          this.metadata == that.metadata
      case _ => super.equals(that)
    }
}

object Entry {
  private val EndpointPrefix = "member_"

  /**
   * Parse a JSON response from ZooKeeper into a Seq[Entry].
   */
  def parseJson(path: String, json: String): Seq[Entry] = {
    val basename = path.split("/").last

    if (basename startsWith EndpointPrefix)
      Endpoint.parseJson(json) map (_.copy(memberId = basename))
    else
      Nil
  }
}

object Endpoint {
  val Empty =
    Endpoint(null, null, Int.MinValue, Int.MinValue, Endpoint.Status.Unknown, "", Map.empty)

  object Status extends Enumeration {
    val Dead, Starting, Alive, Stopping, Stopped, Warning, Unknown = Value

    private val map = Map(
      "DEAD" -> Dead,
      "STARTING" -> Starting,
      "ALIVE" -> Alive,
      "STOPPING" -> Stopping,
      "STOPPED" -> Stopped,
      "WARNING" -> Warning,
      "UNKNOWN" -> Unknown
    )

    def ofString(s: String): Option[Value] = map.get(s)
  }

  private def parseEndpoint(m: Any): Option[(String, Int)] =
    m match {
      case ep: java.util.Map[_, _] =>
        val p = Option(ep.get("port")) collect {
          case port: java.lang.Integer => port
        }

        val h = Option(ep.get("host")) collect {
          case host: String => host
        }

        for (h <- h; p <- p)
          yield (h, p.toInt)

      case _ => None
    }

  def parseJson(json: String): Seq[Endpoint] = {
    val d = JsonDict(json)

    val shard = for { IntObj(s) <- d("shard") } yield s
    val metadata = for (ExtractMetadata(m) <- d("metadata")) yield m
    val status = {
      for {
        StringObj(s) <- d("status")
        status <- Status.ofString(s)
      } yield status
    } getOrElse Endpoint.Status.Unknown
    val tmpl = Endpoint.Empty.copy(
      shard = shard.getOrElse(Int.MinValue),
      status = status,
      metadata = metadata.getOrElse(Map.empty))

    val namesByHostPort =
      Memoize.snappable[(String, Int), ArrayBuffer[String]] {
        case (host, port) =>
          new ArrayBuffer[String]
      }
    for (map <- d("serviceEndpoint"); hostport <- parseEndpoint(map))
      namesByHostPort(hostport) += null
    for {
      map <- d("additionalEndpoints") collect {
        case m: java.util.Map[_, _] => m
      }
      key <- map.keySet().asScala collect { case k: String => k }
      if key.isInstanceOf[String]
      hostport <- parseEndpoint(map.get(key))
    } namesByHostPort(hostport) += key

    for (((host, port), names) <- namesByHostPort.snap.toSeq)
      yield tmpl.copy(names = names.toArray, host = host, port = port)
  }
}
