package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.finagle.mysql.codec.{PacketFrameDecoder, Endec}  
import com.twitter.finagle.mysql.protocol._
import com.twitter.finagle.mysql.protocol.Capability._
import com.twitter.util.Future
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels, Channel}

class MySQL(username: String, password: String, database: Option[String]) 
  extends CodecFactory[Request, Result] {
    private[this] val clientCapability = Capability(
      LongFlag, 
      Transactions, 
      Protocol41, 
      FoundRows, 
      Interactive, 
      LongPassword, 
      ConnectWithDB, 
      SecureConnection, 
      LocalFiles
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
      LoginRequest(username, password, database, clientCap, sg.salt, sg.serverCap)
      
    def acceptGreeting(res: Result) = res match {
      case sg: ServersGreeting if !sg.serverCap.has(Capability.Protocol41) =>
        Future.exception(IncompatibleServer("This client is only compatible with MySQL version 4.1 and later."))

      case sg: ServersGreeting if !Charset.isUTF8(sg.charset) => 
        Future.exception(IncompatibleServer("This client is only compatible with UTF-8 charset encoding."))

      case sg: ServersGreeting =>
        Future.value(sg)

      case r =>
       Future.exception(new ClientError("Invalid Reply type %s".format(r.getClass.getName)))
    }

    def acceptLogin(res: Result) = res match {
      case r: OK => 
        Future.value(res)

      case Error(c, _, m) => 
        Future.exception(ServerError("Error when authenticating the client "+ c + " - " + m))
    }

    override def apply(conn: ClientConnection) = for {
      service <- self(conn)
      result <- service(ClientInternalGreet)
      sg <- acceptGreeting(result)
      loginRes <- service(makeLoginReq(sg))
      _ <- acceptLogin(loginRes)
    } yield service
}