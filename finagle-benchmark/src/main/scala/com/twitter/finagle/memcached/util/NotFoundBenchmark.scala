package com.twitter.finagle.memcached.util

import com.twitter.finagle.benchmark.StdBenchAnnotations
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
class NotFoundBenchmark extends StdBenchAnnotations {

  private[this] val Text =
    "Beard you probably haven't heard of them artisan cronut fanny pack Bespoke organic wolf put " +
      "a bird on it umami. Migas ethical slow-carb YOLO, Thundercats kitsch hella fanny pack VHS " +
      "single-origin coffee stumptown plaid food truck tote bag. Pop-up tofu single-origin coffee " +
      "retro readymade, +1 Intelligentsia Vice paleo listicle selfies vinyl. Lo-fi Portland pork " +
      "belly keffiyeh direct trade. Fanny pack bitters 90's roof party pug swag, health goth Godard " +
      "occupy organic blog brunch trust fund asymmetrical butcher. Small batch Wes Anderson Marfa, " +
      "single-origin coffee meh Etsy biodiesel master cleanse chia church-key slow-carb. Kogi " +
      "shabby chic gluten-free Intelligentsia food truck occupy, Bushwick you probably haven't " +
      "heard of them letterpress. Pug small batch Pinterest, Intelligentsia tofu ugh Tumblr. " +
      "Sustainable Portland meditation locavore Tumblr keffiyeh. Williamsburg ugh direct trade " +
      "actually. Artisan Portland pour-over, High Life umami sustainable cred single-origin coffee " +
      "post-ironic fashion axe wayfarers 90's. Keytar whatever Helvetica, wolf banh mi Etsy " +
      "Brooklyn Marfa post-ironic Intelligentsia. Tote bag ennui try-hard kogi, messenger bag " +
      "Portland kale chips brunch XOXO bitters cronut Wes Anderson."

  private[this] val Keys: Set[String] = Text.split(" ").toSet.take(25)

  private[this] val KVs: Map[String, String] =
    Keys.map { k => k -> k }.toMap

  private def pick(percent: Double): Set[String] = {
    val num = (Keys.size * percent).toInt
    Keys.take(num)
  }

  private[this] val Hits90 = pick(0.9)
  private[this] val HitsCutoff = pick(NotFound.cutoff)

  private[this] val notFound = new NotFound(NotFound.cutoff)

  def baselineSet(toRemove: Set[String]): Set[String] =
    Keys -- toRemove

  def notFoundSet(toRemove: Set[String]): Set[String] =
    notFound(Keys, toRemove)

  def baselineMap(toRemove: Set[String]): Map[String, String] =
    KVs -- toRemove

  def notFoundMap(toRemove: Set[String]): Map[String, String] =
    notFound(KVs, toRemove)

  @Benchmark
  def set_90_Baseline: Set[String] =
    baselineSet(Hits90)

  @Benchmark
  def set_Cutoff_Baseline: Set[String] =
    baselineSet(HitsCutoff)

  @Benchmark
  def set_90_NotFound: Set[String] =
    notFoundSet(Hits90)

  @Benchmark
  def set_Cutoff_NotFound: Set[String] =
    notFoundSet(HitsCutoff)
  @Benchmark
  def map_90_Baseline: Map[String, String] =
    baselineMap(Hits90)

  @Benchmark
  def map_Cutoff_Baseline: Map[String, String] =
    baselineMap(HitsCutoff)

  @Benchmark
  def map_90_NotFound: Map[String, String] =
    notFoundMap(Hits90)

  @Benchmark
  def map_Cutoff_NotFound: Map[String, String] =
    notFoundMap(HitsCutoff)

}
