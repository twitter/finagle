package com.twitter.finagle.stats

import com.twitter.finagle.benchmark.StdBenchAnnotations
import com.twitter.finagle.stats.JsonExporter.commaSeparatedRegex
import java.util.regex.Pattern
import org.openjdk.jmh.annotations._
import scala.annotation.switch

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
class GlobBenchmark extends StdBenchAnnotations {

  private def commaSeparatedGlob(glob: String): Option[Pattern] =
    if (glob.isEmpty) None
    else {
      var i = 0
      // We expand the resulting string to fit up to 8 regex characters w/o
      // resizing the string builder.
      val result = new StringBuilder(glob.length + 8)
      while (i < glob.length) {
        (glob.charAt(i): @switch) match {
          case '[' => result.append("\\[")
          case ']' => result.append("\\]")
          case '|' => result.append("\\|")
          case '^' => result.append("\\^")
          case '$' => result.append("\\$")
          case '.' => result.append("\\.")
          case '?' => result.append("\\?")
          case '+' => result.append("\\+")
          case '(' => result.append("\\(")
          case ')' => result.append("\\)")
          case '{' => result.append("\\{")
          case '}' => result.append("\\}")
          case '*' => result.append(".*")
          case c => result.append(c)
        }
        i += 1
      }

      commaSeparatedRegex(result.toString).map(_.pattern)
    }

  @Benchmark
  def regexp(): Boolean = {
    commaSeparatedGlob("foo*bar,foo*baz").get.matcher("fooXXXbarXXXbaz").matches()
  }

  @Benchmark
  def glob(): Boolean = {
    Glob("foo*bar,foo*baz")("fooXXXbarXXXbaz")
  }
}
