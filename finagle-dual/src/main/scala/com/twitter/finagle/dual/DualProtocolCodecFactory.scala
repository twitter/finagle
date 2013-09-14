/**
 * Copyright 2012-2013  Foursquare Labs, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.finagle.dual

import com.twitter.finagle.{ClientConnection, Codec, CodecFactory, ServerCodecConfig, Service, ServiceFactory}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Future, Time}
import org.jboss.netty.channel.{ChannelPipeline, ChannelPipelineFactory}
import scalaj.collection.Imports._

/**
 * A Finagle [[com.twitter.finagle.CodecFactory]] that makes [[com.twitter.finagle.Codec]] instances that understand
 * how to decode both HTTP and non-HTTP.
 */
class DualProtocolCodecFactory[HttpRequestType <: Request, OtherReq, OtherRep](
      httpFactory: CodecFactory[HttpRequestType, Response],
      regularFactory: CodecFactory[OtherReq, OtherRep])
  extends CodecFactory[AnyRef, AnyRef] {

  override def client: DualProtocolCodecFactory[HttpRequestType, OtherReq, OtherRep]#Client =
    throw new UnsupportedOperationException("Please use the Codec for the specific protocol you want directly.")

  override def server: DualProtocolCodecFactory[HttpRequestType, OtherReq, OtherRep]#Server = {
    { (config: ServerCodecConfig) =>
      new Codec[AnyRef, AnyRef] {
        private val httpCodec = httpFactory.server(config)
        private val regularCodec = regularFactory.server(config)

        override def pipelineFactory: ChannelPipelineFactory = new ChannelPipelineFactory {
          override def getPipeline: ChannelPipeline = {
            // Obtain the HTTP and non-HTTP pipelines. Uses same technique as RichHttp does to obtain the pipeline
            // from another Codec.
            val httpPipeline = httpCodec.pipelineFactory.getPipeline
            val regularPipeline = regularCodec.pipelineFactory.getPipeline

            // Record the names used in the other pipeline. (These will need to be removed from the pipeline when HTTP
            // is detected.)
            val regularHandlers = regularPipeline.toMap.asScala.map { case (_, v) => v }.toSeq
            regularPipeline.addFirst("dualProtocolFrameDecoder",
              new DualProtocolFrameDecoder("dualProtocolFrameDecoder", httpPipeline, regularHandlers))

            regularPipeline
          }
        }

        override def prepareConnFactory(underlying: ServiceFactory[AnyRef, AnyRef]): ServiceFactory[AnyRef, AnyRef] = {
          val httpServiceFactory =
            httpCodec.prepareConnFactory(underlying.asInstanceOf[ServiceFactory[HttpRequestType, Response]])
          val regularServiceFactory =
            regularCodec.prepareConnFactory(underlying.asInstanceOf[ServiceFactory[OtherReq, OtherRep]])

          new ServiceFactory[AnyRef, AnyRef] {
            override def apply(conn: ClientConnection): Future[Service[AnyRef, AnyRef]] = {
              val httpServiceF = httpServiceFactory(conn)
              val regularServiceF = regularServiceFactory(conn)

              Future.collect(Seq(httpServiceF, regularServiceF)) map { services =>
                val httpService = services(0).asInstanceOf[Service[HttpRequestType, Response]]
                val regularService = services(1).asInstanceOf[Service[OtherReq, OtherRep]]
                new Service[AnyRef, AnyRef] {
                  def apply(request: AnyRef): Future[AnyRef] = (request match {
                    case httpRequest: Request => httpService(httpRequest.asInstanceOf[HttpRequestType])
                    case _ => regularService(request.asInstanceOf[OtherReq])
                  }) map { r => r.asInstanceOf[AnyRef] }
                }
              }
            }

            override def close(deadline: Time): Future[Unit] = {
              Future.join(Seq(httpServiceFactory.close(deadline), regularServiceFactory.close(deadline)))
            }
          }
        }
      }  // new Codec
    } // ServerCodecConfig => Codec
  }
}
