package com.twitter.finagle.mysql

import org.jboss.netty.channel._
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.{ServiceFactory, Codec, CodecFactory}
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.util.Future

object Client {

  def apply(factory: ServiceFactory[Request, Result]): Client = {
    new Client(factory())
  }

  /**
   * Construct a client from a single host.
   * @param host a String of host:port combination.
   * @param username the username used to authenticate to the mysql instance
   * @param password the password used to authenticate to the mysql instance
   */
  def apply(host: String, username: String, password: String): Client = {
    val factory = ClientBuilder()
      .codec(new MySQL(username, password))
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory()
      
      apply(factory)
  }

  class Client(fService: Future[Service[Request, Result]]) {
    def use(dbName: String) = send(Use(dbName))
    def query(query: String) = send(Query(query))
    def create(dbName: String) = send(CreateDb(dbName))
    def drop(dbName: String) = send(DropDb(dbName))
    def close() = fService onSuccess { _.release() }

    private[this] def send(r: Request) = fService flatMap { _(r) }
  }

}
