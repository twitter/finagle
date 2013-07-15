package com.twitter.finagle.exp.mysql

import com.twitter.finagle._
import com.twitter.finagle.exp.mysql.codec.{PacketFrameDecoder, Endec}
import com.twitter.finagle.exp.mysql.protocol.{Capability, Charset, ServersGreeting, LoginRequest}
import com.twitter.util.Future
import org.jboss.netty.channel.{Channels, ChannelPipelineFactory}
import org.jboss.netty.handler.codec.frame.FrameDecoder

class MySQL(username: String, password: String, database: Option[String])
  extends CodecFactory[Request, Result] {
    private[this] val clientCapability = Capability(
      Capability.LongFlag,
      Capability.Transactions,
      Capability.Protocol41,
      Capability.FoundRows,
      Capability.Interactive,
      Capability.LongPassword,
      Capability.ConnectWithDB,
      Capability.SecureConnection,
      Capability.LocalFiles
    )

    def server = throw new Exception("Not yet implemented...")

    def client = Function.const {
      new Codec[Request, Result] {

        def pipelineFactory = new ChannelPipelineFactory {
          def getPipeline = {
            val pipeline = Channels.pipeline()

            pipeline.addLast("frameDecoder", new PacketFrameDecoder)
            pipeline.addLast("EncoderDecoder", new Endec)

            pipeline
          }
        }

        // Authenticate each connection before returning it via a ServiceFactoryProxy.
        override def prepareConnFactory(underlying: ServiceFactory[Request, Result]) =
          new AuthenticationProxy(underlying, username, password, database, clientCapability)

      }
    }
}

class AuthenticationProxy(
    underlying: ServiceFactory[Request, Result],
    username: String,
    password: String,
    database: Option[String],
    clientCap: Capability)
  extends ServiceFactoryProxy(underlying) {

    def makeLoginReq(sg: ServersGreeting) =
      LoginRequest(username, password, database, clientCap, sg.salt, sg.serverCap, sg.charset)

    def acceptGreeting(res: Result) = res match {
      case sg: ServersGreeting if !sg.serverCap.has(Capability.Protocol41) =>
        Future.exception(IncompatibleServer("This client is only compatible with MySQL version 4.1 and later."))

      case sg: ServersGreeting if !Charset.isCompatible(sg.charset) =>
        Future.exception(IncompatibleServer("This client is only compatible with UTF-8 and Latin-1 charset encoding."))

      case sg: ServersGreeting =>
        Future.value(sg)

      case r =>
       Future.exception(ClientError("Invalid Reply type %s".format(r.getClass.getName)))
    }

    def acceptLogin(res: Result) = res match {
      case r: OK =>
        Future.value(r)

      case Error(code, state, msg) =>
        Future.exception(ServerError(code, state, msg))

      case uknown =>
        Future.exception(ClientError("Invalid login response type: %s".format(uknown.toString)))
    }

    override def apply(conn: ClientConnection) = for {
      service <- self(conn)
      result <- service(ClientInternalGreet)
      sg <- acceptGreeting(result)
      loginRes <- service(makeLoginReq(sg))
      _ <- acceptLogin(loginRes)
    } yield service
}
