package com.twitter.finagle.mysql

import org.jboss.netty.channel._
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.{ServiceFactory, Codec, CodecFactory}
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import java.net.InetSocketAddress
import com.twitter.util.Future

trait Packet {
  def size: Int
  def number: Byte
  def data: Array[Byte]
}

class MySQLCodec(login: String, password: String) extends CodecFactory[Request, Result] {

  def server = throw new Exception("Not yet implemented...")

  def client = Function.const {
    new Codec[Request, Result] {

      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("decoder", new Decoder)
          pipeline.addLast("encoder", Encoder)
          pipeline.addLast("authentication", new AuthenticationHandler(login, password))

          pipeline
        }
      }
    }
  }
}

object MySQLClient {
  def apply(factory: ServiceFactory[Request, Result]): MySQLClient = {
    new MySQLClient(factory.make())
  }

  def apply(hosts: InetSocketAddress, login: String, password: String): MySQLClient = {
    val factory = ClientBuilder()
      .codec(new MySQLCodec(login, password))
      .hosts(hosts)
      .hostConnectionLimit(1)
      .buildFactory()
    apply(factory)
  }
}

class MySQLClient(fService: Future[Service[Request, Result]]) {
  def use(dbName: String) = send(Use(dbName))
  def query(query: String) = send(Query(query))
  def create(dbName: String) = send(CreateDb(dbName))
  def drop(dbName: String) = send(DropDb(dbName))
  def close() = fService onSuccess { _.release() }

  private[this] def send(r: Request) = fService flatMap { _(r) }
}
