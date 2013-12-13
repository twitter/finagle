package com.twitter.finagle.exp.mysql

case class PreparedStatement(metadata: PrepareOK) {
  private[this] var params: Array[Any] = new Array[Any](numberOfParams)
  private[this] var hasNewParams: Boolean = false

  val numberOfParams: Int = metadata.numOfParams
  val statementId: Int = metadata.statementId
  def parameters: Array[Any] = params
  def hasNewParameters: Boolean = hasNewParams

  def bindParameters() { hasNewParams = false }

  def parameters_=(arr: Array[Any]) = {
    require(arr.size == numberOfParams, "Invalid number of parameters.")
    hasNewParams = true
    params = arr
  }

  def updateParameter(index: Int, value: Any): Unit = {
    hasNewParams = true
    params(index) = value
  }
}