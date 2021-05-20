package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.{
  Address,
  ClientConnection,
  NoBrokersAvailableException,
  Service,
  ServiceFactoryProxy
}
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.FromInstanceId
import com.twitter.finagle.stats.{Counter, InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Rng
import com.twitter.util.{Future, Time}
import org.scalacheck.Gen
import org.scalactic.TolerantNumerics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.math.abs

class WeightedApertureTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private val rng = Rng(123)

  def histo(seq: Seq[Int]): Map[Int, Int] =
    seq.foldLeft(Map.empty[Int, Int]) {
      case (map, i) => map + (i -> (map.getOrElse(i, 0) + 1))
    }

  val numRuns = 100000
  def run(mk: => Seq[Int]): Map[Int, Int] = histo(Seq.fill(numRuns)(mk).flatten)

  def approxEqual(a: Double, b: Double, eps: Double = 0.0001): Boolean = {
    if (abs(a - b) < eps) true
    else false
  }

  def newFactory(equalWeight: Boolean = false): EndpointFactory[Unit, Unit] = {
    new EndpointFactory[Unit, Unit] {
      val address: Address = Address.Failed(new Exception)

      //random weight
      override lazy val weight = 0.1 * (if (equalWeight) 1; else rng.nextDouble())

      def remake(): Unit = {}

      def apply(conn: ClientConnection): Future[Service[Unit, Unit]] =
        Future.value(new Service[Unit, Unit] {
          def apply(req: Unit): Future[Unit] = Future.Done
        })

      def close(when: Time): Future[Unit] = Future.Done
    }
  }

  case class TestNode(factory: EndpointFactory[Unit, Unit])
      extends ServiceFactoryProxy(factory)
      with ApertureNode[Unit, Unit] {
    def tokenRng: Rng = rng
    def load: Double = 0.0
    def pending: Int = 0
  }

  class TestAperture() extends Aperture[Unit, Unit] {
    override type Node = TestNode
    override private[aperture] def rng = WeightedApertureTest.this.rng
    override private[aperture] def minAperture = 12
    override protected val useDeterministicOrdering: Option[Boolean] = Some(true)
    override private[aperture] def eagerConnections = true
    override protected def label: String = ""
    override protected def maxEffort: Int = 0
    override protected def emptyException: Throwable = new NoBrokersAvailableException
    override protected def statsReceiver: StatsReceiver = new InMemoryStatsReceiver
    override protected def newNode(factory: EndpointFactory[Unit, Unit]): TestNode = TestNode(
      newFactory())
    override protected[this] def maxEffortExhausted: Counter =
      statsReceiver.counter("max_effort_exhausted")
  }

  test("WeightedAperture.normalize correctly normalizes a seq of doubles") {
    val weights = Seq(0.5, 1.0, 1.5, 2.0)
    val result = WeightedAperture.normalize(weights)

    assert(result == Seq(0.1, 0.2, 0.3, 0.4))
  }

  test("WeightedAperture.normalize throws exception for negative weights") {
    val weights = Seq(0.5, -1.0, 1.5, 2.0)

    intercept[AssertionError] {
      WeightedAperture.normalize(weights)
    }
  }

  test("WeightedAperture.adjustWeightsWithCoord can wrap around") {
    // Only add 10 nodes so that we are forced to use the MinDeterministicAperture, 12
    val nodes: Vector[TestNode] = Vector.fill(10)(new TestNode(newFactory()))

    // This is a deterministic test, the values below identify the expected raw node weights
    // as well as their normalized values. These values can be useful for debugging.
    /** val nodes = Vector(0.08754127852514175, 0.07160485112997249, 0.007191702249367171,
     * 0.07962609718390336, 0.05787169373422368, 0.09081256181340881, 0.014891457880475457,
     * 0.0975219897302887, 0.006559603048899776, 0.0069517882499888), sum = 0.5205730235 */

    /** val normalizedNodes = Vector(0.1681633019, 0.137550061, 0.01381497297,
     * 0.1529585545, 0.1111692138, 0.1744473066, 0.02860589621,
     * 0.1873358498, 0.01260073564, 0.01335410775) */

    val wts =
      WeightedAperture.adjustWeights(nodes.map(_.factory.weight), FromInstanceId(1, 4))
    val indices = wts.zipWithIndex.collect { case (weight, i) if weight > 0.0 => i }
    assert(indices.size == 10)
    // Note that node 1 is the first node in the aperture, despite this ordering.
    assert(indices == IndexedSeq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

    assert(approxEqual(wts.sum, 1.0))
  }

  test("WeightedAperture.adjustWeights creates reasonable subsets (no wraparound)") {
    // Lets create more than MinDeterministicAperture nodes
    val nodes: Vector[TestNode] = Vector.fill(15)(new TestNode(newFactory()))

    // This is a deterministic test, the values below identify the expected raw node weights
    // as well as their normalized values. These values can be useful for debugging.
    /** val nodes = Vector(0.07847762772691541, 0.0014420992718648718, 0.023449486809082966,
     * 0.04768796803684853, 0.014210005154397954, 0.043948778046512794,
     * 0.011201817816663974, 0.08489001106509408, 0.01566735349119557,
     * 0.04004197185766193, 0.09392759754332235, 0.04163639795243517,
     * 0.08285576943871914, 0.06629799243018646, 0.003081362527566656) */

    /** val normalizedNodes = Vector(0.1209550917, 0.002222662111, 0.03614195421,
     * 0.07349996063, 0.02190143263, 0.06773686506,
     * 0.0172650084, 0.1308382959, 0.02414759765,
     * 0.06171542794, 0.1447676428, 0.06417286658,
     * 0.1277029834, 0.1021830041, 0.00474920685) */

    val wts =
      WeightedAperture.adjustWeights(nodes.map(_.factory.weight), FromInstanceId(1, 5))
    val is = wts.zipWithIndex.collect { case (weight, i) if weight > 0.0 => i }
    assert(is.size == 12)
    assert(is == IndexedSeq(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))

    assert(approxEqual(wts.sum, 0.8))

    val wts2 =
      WeightedAperture.adjustWeights(nodes.map(_.factory.weight), FromInstanceId(0, 5))
    val is2 = wts2.zipWithIndex.collect { case (weight, i) if weight > 0.0 => i }

    // This will include an addl node due to dApertureWidth = 0.8 ...
    // The cumulative sum to node 11 is only 0.76, so we need to include a small portion of node 12
    assert(is2.size == 13)
    assert(is2 == IndexedSeq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))

    assert(approxEqual(wts2.sum, 0.8))
  }

  test("reproduces Ring behavior when nodes are equally weighted") {
    val endpoints: Vector[TestNode] = Vector.fill(10)(TestNode(newFactory(equalWeight = true)))

    val wap = new WeightedAperture[Unit, Unit, TestNode](
      aperture = new TestAperture(),
      endpoints = endpoints,
      initAperture = 4,
      coord = FromInstanceId(2, 4))

    val ring = new Ring(10, rng)
    val offset = 3 / 4d
    val width = 1 / 4d

    val histo0 = run {
      val (a, b) = ring.pick2(offset, width)
      if (a == b) {
        fail(s"ring pick: (a=$a, b=$b)")
      }
      Seq(a, b)
    }

    val histo1 = run {
      val a = wap.pdist.pickOne()
      val b = wap.pdist.tryPickSecond(a)
      Seq(a, b)
    }

    for (key <- histo0.keys) {
      val a = histo0(key)
      val b = histo1(key)
      assert(math.abs(a - b) / math.max(a, b) < 1e-3)
    }
  }

  // Property testing methods
  private def getNormalized(testParams: TestParams): IndexedSeq[Double] =
    WeightedAperture
      .adjustWeights(
        Vector.fill(testParams.remoteClusterSize)(1.0),
        testParams.offset,
        testParams.width)

  private def getWeightMap(testParams: TestParams): Map[Int, Double] =
    getNormalized(testParams).zipWithIndex.collect {
      case (weight, i) if weight > 0.0 =>
        // We normalize the weights differently but it's easy to correct
        i -> (weight * testParams.remoteClusterSize)
    }.toMap

  private def getRingWeightMap(testParams: TestParams): Map[Int, Double] = {
    val ring = new Ring(testParams.remoteClusterSize, null)
    ring
      .indices(testParams.offset, testParams.width).map { i =>
        i -> ring.weight(i, testParams.offset, testParams.width)
      }.toMap
  }

  case class TestParams(offset: Double, width: Double, remoteClusterSize: Int)

  private val epsilon = 1e-4
  // Allow some tolerance to our `===` operator to facilitate floating point error
  private implicit val doubleEq = TolerantNumerics.tolerantDoubleEquality(epsilon)

  private val testGen: Gen[TestParams] = for {
    remoteClusterSize <- Gen.chooseNum(1, 2000)
    localClusterSize <- Gen.chooseNum(1, 2000)
    width <- Gen.chooseNum(1.0 / localClusterSize, 1.0)
    offset <- Gen.chooseNum(0.0, 1.0 - epsilon)
  } yield TestParams(offset, width, remoteClusterSize)

  test("Result parity for Ring and weighted aperture prefilter") {
    forAll(testGen) { testParams =>
      val normalizedWeights = getWeightMap(testParams)
      val ringWeights = getRingWeightMap(testParams)
      assert(normalizedWeights.keySet == ringWeights.keySet)

      // We have the same set of weights now make sure the weights correspond
      normalizedWeights.foreach {
        case (i, weight) => assert(weight === ringWeights(i))
      }
    }
  }
}
