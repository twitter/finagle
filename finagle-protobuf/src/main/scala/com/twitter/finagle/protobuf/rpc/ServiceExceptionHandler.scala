package com.twitter.finagle.protobuf.rpc

trait ServiceExceptionHandler[T] {

  def canHandle(e: RuntimeException): Boolean;

  def handle(e: RuntimeException): T;
  
}