package com.twitter.finagle.redis.protocol

import com.twitter.io.{Buf, ByteReader}
import java.nio.charset.StandardCharsets

/**
 * A decoder stage.
 */
private[redis] trait Stage {
  def apply(reader: ByteReader): Stage.NextStep
}

private[redis] object Stage {

  private[this] final val CR = Buf.Utf8("\r")

  /**
   * The next step for processing after this stage.
   *
   * - Incomplete: Need more data; call the same stage again later when more data
   *   arrives.
   *
   * - GoToStage(stage): Finished with this stage; continue with another stage.
   *
   * - Emit(obj): Complete protocol object decoded; return to the first stage and
   *   start a new object.
   *
   * - Accumulate(n, finish): The `n`-step accumulation process should begin.
   */
  sealed trait NextStep

  object NextStep {
    case object Incomplete extends NextStep
    case class Goto(stage: Stage) extends NextStep
    case class Emit(a: Reply) extends NextStep
    case class Accumulate(n: Long, finish: List[Reply] => Reply) extends NextStep
  }

  /**
   * Generate a Stage from a code block.
   */
  def apply(f: ByteReader => NextStep): Stage = new Stage {
    def apply(buf: ByteReader): NextStep = f(buf)
  }

  /**
   * Generates a const Stage.
   */
  def const(next: NextStep): Stage = apply(_ => next)

  /**
   * Read `count` bytes into a byte buffer and pass that buffer to the next step in
   * processing.
   */
  def readBytes(count: Int)(process: Buf => NextStep): Stage = Stage { reader =>
    if (reader.remaining < count) {
      NextStep.Incomplete
    } else {
      process(reader.readBytes(count))
    }
  }

  /**
   * Read a line, terminated by LF or CR/LF and pass that line (stripping the CL/RF bytes)
   * as a UTF-8 string to the next processing step.
   */
  def readLine(process: String => NextStep): Stage = Stage { reader =>
    val untilNewLine = reader.remainingUntil('\n')
    if (untilNewLine == -1) NextStep.Incomplete
    else {
      val line = reader.readBytes(untilNewLine) match {
        case Buf.Empty => ""
        case buf =>
          val bytes = Buf.ByteArray.Owned.extract(buf)
          if (bytes(bytes.length - 1) == '\r')
            new String(bytes, 0, bytes.length - 1, StandardCharsets.UTF_8)
          else
            new String(bytes, StandardCharsets.UTF_8)
      }

      reader.skip(1) // skip LF

      process(line)
    }
  }
}
