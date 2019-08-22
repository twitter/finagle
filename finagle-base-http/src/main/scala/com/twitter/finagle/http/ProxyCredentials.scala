package com.twitter.finagle.http

import com.twitter.util.Base64StringEncoder
import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

object ProxyCredentials {
  def apply(credentials: java.util.Map[String, String]): Option[ProxyCredentials] =
    apply(credentials.asScala.toMap)

  def apply(credentials: Map[String, String]): Option[ProxyCredentials] = {
    for {
      user <- credentials.get("http_proxy_user")
      pass <- credentials.get("http_proxy_pass")
    } yield {
      ProxyCredentials(user, pass)
    }
  }
}

case class ProxyCredentials(username: String, password: String) {
  lazy val basicAuthorization: String = {
    val bytes = "%s:%s".format(username, password).getBytes(StandardCharsets.UTF_8)
    "Basic " + Base64StringEncoder.encode(bytes)
  }
}
