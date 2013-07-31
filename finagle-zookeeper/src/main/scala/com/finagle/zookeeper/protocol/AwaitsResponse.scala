package com.finagle.zookeeper.protocol

trait AwaitsResponse {

  def responseDeserializer: HeaderBodyDeserializer

}
