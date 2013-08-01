package com.finagle.zookeeper.protocol

trait ExpectsResponse {

  def responseDeserializer: HeaderBodyDeserializer

}
