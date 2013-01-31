package com.twitter.finagle.protobuf.rpc

class RpcControllerWithOnFailureCallback extends RpcController {

  private var cancelRequested = false

  private var callaback: RpcCallback[Throwable] = null

  def reset(): Unit = {
    cancelRequested = false
  }

  def failed(): Boolean = { throw new RuntimeException("Not implemented") }

  def errorText(): String = { throw new RuntimeException("Not implemented") }

  def startCancel(): Unit = { cancelRequested = true; }

  def setFailed(reason: String): Unit = { throw new RuntimeException("Not implemented") }

  def setFailed(e: Throwable): Unit = {
    callaback.run(adapt(e))
  }

  def isCanceled() = cancelRequested;

  def notifyOnCancel(callback: RpcCallback[Object]): Unit = { throw new RuntimeException("Not implemented") }

  def onFailure(callback: RpcCallback[Throwable]): RpcControllerWithOnFailureCallback = {
    this.callaback = callback
    this
  }

  def adapt(e: Throwable): Throwable = {
    e match {
      case _: TimeoutException => {
        def wrapped = new java.util.concurrent.TimeoutException(e.getMessage())
        wrapped.initCause(e)
        wrapped
      }
      case _: ChannelClosedException => return new RuntimeException(e)
      case _ => e
    }
  }
}
