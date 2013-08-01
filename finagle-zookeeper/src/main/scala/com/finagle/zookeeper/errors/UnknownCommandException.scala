package com.finagle.zookeeper.errors

case class UnknownCommandException(message: String = "") extends Exception(message)
