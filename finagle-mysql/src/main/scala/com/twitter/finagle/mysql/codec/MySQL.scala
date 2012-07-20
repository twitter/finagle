package com.twitter.finagle.mysql.codec

import com.twitter.finagle._
import com.twitter.finagle.mysql._  
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
          pipeline.addLast("resultDecoder", new ResultDecoder)
          pipeline.addLast("requestEncoder", RequestEncoder)

          pipeline
        }
      }

      /* Authenticate each connection before returning it via a ServiceFactoryProxy. */
      override def prepareConnFactory(underlying: ServiceFactory[Request, Result]) = 
        new AuthenticationProxy(underlying, username, password, database)

    }
  }
}

class AuthenticationProxy(underlying: ServiceFactory[Request, Result], 
                          username: String, 
                          password: String,
                          database: Option[String]) 
  extends ServiceFactoryProxy(underlying) {
  val greet = new SimpleRequest(Command.COM_NOOP_GREET)

  def makeLoginReq(sg: ServersGreeting) = LoginRequest(
            username = username,
            password = password,
            database = database,
            serverCap = sg.serverCap,
            salt = sg.salt
          )

  override def apply(conn: ClientConnection) = {
    self(conn) flatMap { service => service(greet) flatMap { 
        case sg: ServersGreeting if sg.serverCap.has(Capability.protocol41) =>
          Future.value(sg)

        case sg: ServersGreeting => 
          Future.exception(IncompatibleServerVersion)
        } flatMap { sg => service(makeLoginReq(sg)) flatMap {
            case result: OK => 
              Future.value(service)

            case Error(c, s, m) => 
              Future.exception(ServerError("Error when authenticating the client "+ c + " - " + m))  
          }
        }
      }
    }
}