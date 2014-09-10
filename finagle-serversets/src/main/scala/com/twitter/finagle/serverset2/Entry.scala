package com.twitter.finagle.serverset2

import collection.JavaConverters._
import collection.mutable.ArrayBuffer
import java.net.InetSocketAddress

/** 
 * Represents one logical serverset2 entry.
 */
sealed trait Entry

/**
 * Represents an Endpoint's host address and port.
 */
case class HostPort(host: String, port: Int)

/** 
 * Endpoints encode a destination announced via serversets.
 *
 * @param name The endpoint name. None describes a default service
 * endpoint.
 *
 * @param addr The address of the endpoint.
 *
 * @param shard The shard id of the endpoint.
 *
 * @param status The endpoint's status.
 *
 * @param memberId The endpoint's member id, 
 * used as a foreign key for endpoints.
 */
case class Endpoint(
  name: Option[String],
  addr: Option[HostPort],
  shard: Option[Int],
  status: Endpoint.Status.Value,
  memberId: String
) extends Entry

object Entry {
  private val EndpointPrefix = "member_"

  def parseJson(path: String, json: String): Seq[Entry] = {
    val basename = path.split("/").last

    if (basename startsWith EndpointPrefix)
      Endpoint.parseJson(json) map(_.copy(memberId=basename))
    else
      Seq.empty
  }
}

object Endpoint {
  val Empty = Endpoint(
    None, None,
    None, Endpoint.Status.Unknown, "")

  object Status extends Enumeration {
    val Dead, Starting, Alive, Stopping, Stopped, Warning, Unknown = Value

    private val map = Map(
      "DEAD" -> Dead,
      "STARTING" -> Starting,
      "ALIVE" -> Alive,
      "STOPPING" -> Stopping,
      "STOPPED" -> Stopped,
      "WARNING" -> Warning,
      "UNKNOWN" -> Unknown)

    def ofString(s: String): Option[Value] = map.get(s)
  }
  
  private def parseEndpoint(m: Any): Option[HostPort] =
    m match {
      case ep: java.util.Map[_, _] =>
        val p = Option(ep.get("port")) collect {
          case port: java.lang.Integer => port
        }

        val h = Option(ep.get("host")) collect {
          case host: String => host
        }

        for (h <- h; p <- p)
          yield HostPort(h, p.toInt)

      case _ => None
    }

  def parseJson(json: String): Seq[Endpoint] = {
    val d = JsonDict(json)

    val shard = for { IntObj(s) <- d("shard") } yield s
    val status = {
      for { 
        StringObj(s) <- d("status")
        status <- Status.ofString(s) 
      } yield status
    } getOrElse Endpoint.Status.Unknown

    val tmpl = Endpoint.Empty.copy(shard=shard, status=status)
    val eps = new ArrayBuffer[Endpoint]

    for (map <- d("serviceEndpoint"); addr <- parseEndpoint(map))
      eps += tmpl.copy(addr=Some(addr))

    for {
      map <- d("additionalEndpoints") collect {
        case m: java.util.Map[_, _] => m
      }
      key <- map.keySet().asScala collect { case k: String => k }
      if key.isInstanceOf[String]
      addr <- parseEndpoint(map.get(key))
    } eps += tmpl.copy(name=Some(key), addr=Some(addr))
    
    eps.result
  }
}
