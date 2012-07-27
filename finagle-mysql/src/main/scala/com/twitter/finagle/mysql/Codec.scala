package com.twitter.finagle.mysql

import com.twitter.finagle._
import com.twitter.finagle.mysql.codec.{PacketFrameDecoder, Endec}  
import com.twitter.finagle.mysql.protocol._
import com.twitter.util.Future
import org.jboss.netty.channel.{ChannelPipelineFactory, Channels, Channel}

class MySQL(username: String, password: String, database: Option[String]) 
  extends CodecFactory[Request, Result] {
      
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
        new AuthenticationProxy(underlying, username, password, database)

    }
  }
}

class AuthenticationProxy(
    underlying: ServiceFactory[Request, Result], 
    username: String, 
    password: String,
    database: Option[String]) 
  extends ServiceFactoryProxy(underlying) {

  def makeLoginReq(sg: ServersGreeting) =
    LoginRequest(username, password, database, sg.serverCap, sg.salt)
    
  def acceptGreeting(res: Result) = res match {
    case sg: ServersGreeting if sg.serverCap.has(Capability.Protocol41) =>
      Future.value(sg)

    case sg: ServersGreeting =>
      Future.exception(IncompatibleServerVersion)

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