package com.twitter.finagle.mysql.protocol

import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import com.twitter.finagle._
import com.twitter.util.Future
import com.twitter.finagle.mysql._

class MySQL(username: String, password: String, database: Option[String]) 
  extends CodecFactory[Request, Result] {
      
  def server = throw new Exception("Not yet implemented...")

  def client = Function.const {
    new Codec[Request, Result] {

      def pipelineFactory = new ChannelPipelineFactory {
        def getPipeline = {
          val pipeline = Channels.pipeline()

          pipeline.addLast("decoder", new Decoder)
          pipeline.addLast("encoder", Encoder)
          pipeline
        }
      }

      /* Wrap each service with a ServiceProxyFactory that handles authentication. */
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

  def makeLoginReq(salt: Array[Byte]) = LoginRequest(
            username = username,
            password = password,
            database = database,
            salt = salt
          )

  override def apply(conn: ClientConnection) = {
    self(conn) flatMap { service => 
      service(Request.greet) flatMap { 
        case sg: ServersGreeting if sg.serverCapability.has(Capability.protocol41) => 
          Future.value(sg)
        case sg: ServersGreeting => 
          Future.exception(IncompatibleServerVersion)
        case r => 
          Future.exception(InvalidResponseException("Expected server greeting and received " + r))
        } flatMap { sg =>
          service(makeLoginReq(sg.salt)) flatMap {
            case OK => Future.value(service)
            case Error(c, s, m) => Future.exception(AuthenticationException("Error Code "+ c + " - " + m))  
          }
        }
      }
    }
}