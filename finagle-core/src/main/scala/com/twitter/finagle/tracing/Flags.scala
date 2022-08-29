package com.twitter.finagle.tracing

object Flags {

  /**
   * Overview of Tracing flags
   * size in bits:
   *  1  2                 32                   8            20          1
   * +-+--+--------------------------------+-------+--------------------+-+
   * | |  |          [[SampleRate]]        |       |      Extension     |0|
   * | |  |                                |       |        Flags       |0|
   * +-+--+--------------------------------+-------+--------------------+-+
   *  |  |                                                               |
   *  |  +-[[SamplingKnown]][[Sampled]]                                  +-Most Significant Bit
   *  +-[[Debug]]
   *
   * Extension Flags:
   *  The [[Sampled]] & [[Debug]] flags which are used to propagate trace sampling decisions
   *  are tightly coupled to the underlying sampling tracer implementation. These flags
   *  cannot be reused by other "non sampling" tracers without triggering unintended side effects
   *  in the existing sampling tracers. Hence the highest 20 bits (positions 43...62) excluding the
   *  most significant bit are reserved for use by custom tracers to propagate their tracing
   *  decisions safely without affecting any existing tracer implementations.
   *
   *  Finagle will never mutate extension flags, so it's safe to set your own extension flags.
   */
  /*
   * The debug flag is used to ensure this the current trace passes
   * all of the sampling stages.
   */
  val Debug = 1L << 0 // 1

  /**
   * Reserved for future use to encode sampling behavior, currently
   * encoded explicitly in TraceId.sampled (Option[Boolean]).
   */
  val SamplingKnown = 1L << 1
  val Sampled = 1L << 2

  /**
   * We reserve 32 bits to encode the sampling rate as a float value. These are the bits
   * immediately succeeding the Sampled flag, hence the shift of 3
   */
  private val SampleRateNumBits: Int = 32
  private val MaxSampleRateFlagsValue: Long = (1L << SampleRateNumBits) - 1
  private[tracing] val SampleRateShift: Int = 3

  /**
   * The SampleRateMask is used to store and propagate the sampling rate to child or non-deciding
   * traces.
   */
  private[tracing] val SampleRateMask = MaxSampleRateFlagsValue << SampleRateShift

  private[this] val Empty: Flags = Flags(0L)

  /**
   * @return a flags instance with no flags set.
   */
  def apply(): Flags = Empty

  /**
   * The most significant [[ExtensionFlagsNumBits]] bits (excluding the sign bit) are reserved
   * for ExtensionFlags which is used to encode tracing decisions of custom tracers.
   */
  private val ExtensionFlagsNumBits: Int = 20
  private val MaxExtensionFlagsValue: Long = (1L << ExtensionFlagsNumBits) - 1

  /**
   * Represents the lowest bit position of extension flags.
   */
  val ExtensionFlagsShift: Int = 43

  /**
   * Bitmask which represents the bit range of extension flags.
   */
  val ExtensionFlagsMask: Long = MaxExtensionFlagsValue << ExtensionFlagsShift
}

/**
 * Represents flags that can be passed along in request headers.
 * @param flags Initial flag state. May be 0.
 */
case class Flags(flags: Long) {

  /**
   * @param field Is this flag set or not? Pass a field identifier from the Flags object.
   */
  def isFlagSet(field: Long): Boolean = { (flags & field) == field }

  /**
   * @param field Field to set. Pass a field identifier from the Flags object.
   * @return a new Flags object with the flag set
   */
  def setFlag(field: Long): Flags = Flags(flags | field)

  /**
   * @param fields Fields to set. Pass field identifiers from the Flags object.
   * @return a new Flags object with the flags set
   */
  def setFlags(fields: Seq[Long]): Flags = {
    Flags(fields.reduceLeft((a, b) => a | b))
  }

  /**
   * Convenience method to check if the debug flag is set.
   */
  def isDebug = isFlagSet(Flags.Debug)

  /**
   * Convenience method to set the debug flag.
   */
  def setDebug: Flags = setFlag(Flags.Debug)

  /**
   * Method to set the value of sampleRate flag
   */
  def setSampleRate(value: Float): Flags = {
    // We convert the float to bytes and then insert them in the right place within the 64 bits (offset by 3)
    if (value < 0 || value > 1)
      throw new IllegalArgumentException(
        "Sample rate not within the valid range of 0-1, was " + value
      )
    else {
      val floatAsInt = java.lang.Float.floatToRawIntBits(value)
      val bitVal = floatAsInt.toLong << Flags.SampleRateShift
      val maskedBitVal = bitVal & Flags.SampleRateMask
      Flags(flags | maskedBitVal)
    }
  }

  /**
   * Method that converts the bits storing the sample rate back into a float
   */
  def getSampleRate(): Float = {
    val unshifted: Long = flags >> Flags.SampleRateShift
    java.lang.Float.intBitsToFloat(unshifted.toInt)
  }

  /**
   * @return a long that we can use to pass in the tracing header
   */
  def toLong = flags
}
