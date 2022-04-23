package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle.Address
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.NoBrokersAvailableException
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactoryProxy
import com.twitter.finagle.Status
import com.twitter.finagle.loadbalancer.EndpointFactory
import com.twitter.finagle.loadbalancer.PanicMode
import com.twitter.finagle.loadbalancer.aperture.ProcessCoordinate.FromInstanceId
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalacheck.Gen
import org.scalactic.TolerantNumerics
import org.scalatest.OneInstancePerTest
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.math.abs

class WeightedApertureTest extends BaseWeightedApertureTest(manageWeights = true)

abstract class BaseWeightedApertureTest(manageWeights: Boolean)
    extends BaseApertureTools(manageWeights)
    with ScalaCheckDrivenPropertyChecks
    with OneInstancePerTest {

  private val rng = Rng(123)

  def histo(seq: Seq[Int]): Map[Int, Int] =
    seq.foldLeft(Map.empty[Int, Int]) {
      case (map, i) => map + (i -> (map.getOrElse(i, 0) + 1))
    }

  def nodehisto(seq: Seq[TestNode]): Map[TestNode, Int] =
    seq.foldLeft(Map.empty[TestNode, Int]) {
      case (map, i) => map + (i -> (map.getOrElse(i, 0) + 1))
    }

  val numRuns = 100000
  def run(mk: => Seq[Int]): Map[Int, Int] = histo(Seq.fill(numRuns)(mk).flatten)
  def noderun(mk: => TestNode): Map[TestNode, Int] = nodehisto(Seq.fill(numRuns)(mk))

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
    override private[aperture] def rng = BaseWeightedApertureTest.this.rng
    override private[aperture] def minAperture = 12
    override protected val useDeterministicOrdering: Option[Boolean] = Some(true)
    override private[aperture] def eagerConnections = true
    override private[aperture] val manageWeights: Boolean =
      BaseWeightedApertureTest.this.manageWeights
    override protected def label: String = ""
    override private[loadbalancer] def panicMode: PanicMode = PanicMode.Paranoid
    override protected def emptyException: Throwable = new NoBrokersAvailableException
    override protected def statsReceiver: StatsReceiver = new InMemoryStatsReceiver
    override protected def newNode(factory: EndpointFactory[Unit, Unit]): TestNode = TestNode(
      newFactory())
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
    val nodes: Vector[TestNode] = Vector.fill(10)(TestNode(newFactory()))

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
    assert(indices == IndexedSeq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9))

    assert(approxEqual(wts.sum, 1.0))
  }

  test("WeightedAperture.adjustWeights creates reasonable subsets (no wraparound)") {
    // Lets create more than MinDeterministicAperture nodes
    val nodes: Vector[TestNode] = Vector.fill(15)(TestNode(newFactory()))

    // This is a deterministic test, the values below identify the expected raw node weights
    // as well as their normalized values. These values can be useful for debugging.
    /** val nodes = Vector(0.06818127601781632, 0.02676535334991952, 0.024259669941850928,
     * 0.060227644665411986, 0.021662557088166313, 0.04581934074401264, 0.008902600511190118,
     * 0.012225295738506748, 0.03381119880745816, 0.09320068787692642, 0.06778865989313106,
     * 1.8821268751749722E-4, 0.04998637327300792, 0.09808325343250059, 0.08018058428232361)
     */

    /** val normalizedNodes = Vector(0.0986300904076233, 0.0387183897820382, 0.035093702837104696,
     * 0.08712447735409877, 0.03133675532132662, 0.06628162428081825, 0.012878378706966257,
     * 0.017684943643967176, 0.048910812321821505, 0.13482282538906853, 0.09806213735460213,
     * 2.722658693108315E-4, 0.07230959587464579, 0.14188587715773285, 0.11598812369887528)
     */

    val wts =
      WeightedAperture.adjustWeights(nodes.map(_.factory.weight), FromInstanceId(1, 5))
    val is = wts.zipWithIndex.collect { case (weight, i) if weight > 0.0 => i }
    assert(is.size == 12)
    assert(is == IndexedSeq(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))

    assert(approxEqual(wts.sum, 0.8))

    val wts2 =
      WeightedAperture.adjustWeights(nodes.map(_.factory.weight), FromInstanceId(0, 5))
    val is2 = wts2.zipWithIndex.collect { case (weight, i) if weight > 0.0 => i }

    // This will include an additional node due to dApertureWidth = 0.8 ...
    // The cumulative sum to node 12 is only 0.74, so we need to include a small portion of node 13
    assert(is2.size == 14)
    assert(is2 == IndexedSeq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))

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

  test("weighted aperture doesn't unduly bias") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val counts = new Counts
        val bal = new Bal {
          override val minAperture = 1
          override protected def nodeLoad: Double = 1.0
          override val useDeterministicOrdering: Option[Boolean] = Some(true)
        }

        ProcessCoordinate.setCoordinate(0, 3)
        bal.update(counts.range(3))
        bal.rebuildx()
        assert(bal.isWeightedAperture)
        bal.applyn(3000)

        val requests = counts.toIterator.map(_._total).toVector
        val avg = requests.sum.toDouble / requests.size
        val relativeDiffs = requests.map { i => math.abs(avg - i) / avg }
        relativeDiffs.foreach { i => assert(i < 0.05) }
      }
  }

  test("can't switch between weighted and unweighted aperture via rebuilds") {
    val bal = new Bal {
      override protected val useDeterministicOrdering = Some(true)
      override val manageWeights: Boolean = false
    }
    ProcessCoordinate.setCoordinate(0, 50)
    bal.update(Vector.tabulate(20)(Factory))
    bal.rebuildx()
    assert(bal.isDeterministicAperture)

    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        bal.update(Vector.tabulate(20)(Factory))
        bal.rebuildx()
        assert(bal.isDeterministicAperture)
        assert(!bal.isWeightedAperture)
      }
  }

  test("weighted aperture avoids unavailable hosts") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val counts = new Counts
        val bal = new Bal

        bal.update(counts.range(10))
        bal.adjustx(3)
        bal.applyn(100)
        assert(counts.nonzero.size == 10)
        assert(bal.isWeightedAperture)

        // Since tokens are assigned, we don't know apriori what's in the
        // aperture*, so figure it out by observation.
        //
        // *Ok, technically we can, since we're using deterministic
        // randomness.
        for (unavailableStatus <- List(Status.Closed, Status.Busy)) {
          val nonZeroKeys = counts.nonzero
          val closed0 = counts(nonZeroKeys.head)
          val closed1 = counts(nonZeroKeys.tail.head)

          closed0.status = unavailableStatus
          closed1.status = unavailableStatus

          val closed0Req = closed0.total
          val closed1Req = closed1.total

          bal.applyn(100)

          // We want to make sure that we haven't sent requests to the
          // `Closed` nodes since our aperture is wide enough to avoid
          // them.
          assert(closed0Req == closed0.total)
          assert(closed1Req == closed1.total)
        }
      }
  }

  test("ignore minaperture") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val bal = new Bal {
          override val minAperture = 150
        }
        ProcessCoordinate.setCoordinate(0, 150)
        bal.update(Vector.tabulate(150)(Factory))
        bal.rebuildx()
        assert(bal.isWeightedAperture)
        assert(bal.minUnitsx == 12)

        // Now unset the coordinate which should send us back to random aperture
        ProcessCoordinate.unsetCoordinate()
        assert(bal.isRandomAperture)
        bal.update(Vector.tabulate(150)(Factory))
        bal.rebuildx()
        assert(bal.minUnitsx == 150)
      }
  }

  test("Empty vectors") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val bal = new Bal
        intercept[Empty] {
          Await.result(bal.apply())
        }

        // transient update
        val counts = new Counts
        ProcessCoordinate.setCoordinate(0, 2)
        bal.update(counts.range(5))
        bal.applyn(100)
        assert(bal.isWeightedAperture)
        assert(counts.nonzero.size > 0)

        bal.update(Vector.empty)
        intercept[Empty] {
          Await.result(bal.apply())
        }
      }
  }

  /**
   * The following tests are for the weighted RandomAperture implementation
   */
  test("weighted RandomAperture") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val endpoints: Vector[TestNode] = Vector.fill(10)(TestNode(newFactory()))
        val sum: Double = endpoints.map(_.factory.weight).sum

        val rap = new RandomAperture[Unit, Unit, TestNode](
          aperture = new TestAperture() {
            override val manageWeights = true
          },
          vector = endpoints,
          initAperture = 10,
        )

        assert(rap.pdist.isDefined)
        val pdist = rap.pdist.get

        val histo0 = run {
          val a = pdist.pickOne()
          val b = pdist.tryPickSecond(a)
          Seq(a, b)
        }

        for (key <- histo0.keys) {
          // the number of times key is selected over total node selections
          val a = histo0(key) / (numRuns * 2d)
          // the normalized weight of that key
          val b = pdist.weight(key) / sum
          assert(math.abs(a - b) / math.max(a, b) < 1.2e-1)
        }
      }
  }

  test("weighted RandomAperture with evenly weighted endpoints") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val endpoints: Vector[TestNode] = Vector.fill(10)(TestNode(newFactory(true)))
        val sum: Double = endpoints.map(_.factory.weight).sum

        val rap = new RandomAperture[Unit, Unit, TestNode](
          aperture = new TestAperture() {
            override val manageWeights = true
          },
          vector = endpoints,
          initAperture = 10,
        )

        assert(rap.pdist.isDefined)
        val pdist = rap.pdist.get

        val histo0 = run {
          val a = pdist.pickOne()
          val b = pdist.tryPickSecond(a)
          Seq(a, b)
        }

        for (key <- histo0.keys) {
          // the number of times key is selected over total node selections
          val a = histo0(key) / (numRuns * 2d)
          // the normalized weight of that key
          val b = (pdist.weight(key) / sum)
          assert(math.abs(a - b) / math.max(a, b) < 1e-1)
        }
      }
  }

  test("weighted RandomAperture with aperture subset") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val endpoints: Vector[TestNode] = Vector.fill(20)(TestNode(newFactory(true)))
        val sum: Double = endpoints.map(_.factory.weight).sum
        val apertureSum: Double = 1 / 20d * 12d

        val rap = new RandomAperture[Unit, Unit, TestNode](
          aperture = new TestAperture() {
            override val manageWeights = true
          },
          vector = endpoints,
          initAperture = 12,
        )

        assert(rap.pdist.isDefined)
        assert(rap.logicalAperture == 12)
        val pdist = rap.pdist.get

        val histo0 = run {
          val a = pdist.pickOne()
          val b = pdist.tryPickSecond(a)
          Seq(a, b)
        }

        for (key <- histo0.keys) {
          // the number of times key is selected over total node selections
          val a = histo0(key) / (numRuns * 2d)
          // the normalized weight of that key is normalized to the aperture size
          val b = (pdist.weight(key) / sum / apertureSum)
          assert(math.abs(a - b) / math.max(a, b) < 1e-1)
        }
      }
  }

  test("RandomAperture.indices works as expected") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val endpoints: Vector[TestNode] = Vector.fill(20)(TestNode(newFactory()))
        val rap = new RandomAperture[Unit, Unit, TestNode](
          aperture = new TestAperture() {
            override val manageWeights = true
          },
          vector = endpoints,
          initAperture = 12,
        )

        val cumulativeProbability = Seq(0.10897957345898437, 0.1268500607086572,
          0.24388128008462095, 0.2517531287369932, 0.26009561906137796, 0.2709863439395602,
          0.32192445432616834, 0.4268557117936775, 0.49587102482185114, 0.5434526136090619,
          0.5744517234237482, 0.5927875387629006, 0.6240404207108955, 0.6794445417803858,
          0.7321388121574698, 0.757508511564142, 0.8312170117679757, 0.8352321269266164,
          0.9179030745947724, 1.0)

        val pdist = rap.pdist.get
        assert(cumulativeProbability == pdist.cumulativeProbability.toSeq)
        val physicalAperture = 12d / 20d
        assert(pdist.scaledAperture == physicalAperture)

        // Because the physical Aperture is 0.6, nodes 0 through 12 should exist within the aperture
        assert(rap.indices == (0 to 12).toSet)
      }
  }

  test("weighted and unweighted pathway parity") {
    val endpoints: Vector[TestNode] = Vector.fill(10)(TestNode(newFactory(true)))
    val wrap = com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        new RandomAperture[Unit, Unit, TestNode](
          aperture = new TestAperture() {
            override val manageWeights = true
          },
          vector = endpoints,
          initAperture = 12,
        )
      }
    val urap = new RandomAperture[Unit, Unit, TestNode](
      aperture = new TestAperture() {
        override val manageWeights = false
      },
      vector = endpoints,
      initAperture = 12,
    )

    assert(wrap.pdist.isDefined)
    assert(urap.pdist.isEmpty)

    val nodehisto1 = noderun {
      wrap.pick()
    }

    val nodehisto2 = noderun {
      urap.pick()
    }

    for (key <- nodehisto1.keys) {
      val a = nodehisto1(key)
      val b = nodehisto2(key)
      assert(math.abs(a - b) / math.max(a, b) < 1e-3)
    }
  }

  test("logical aperture can change") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val endpoints: Vector[TestNode] = Vector.fill(20)(TestNode(newFactory(true)))
        val rap = new RandomAperture[Unit, Unit, TestNode](
          aperture = new TestAperture() {
            override val manageWeights = true
          },
          vector = endpoints,
          initAperture = 12,
        )

        val pdist = rap.pdist.get

        assert(rap.logicalAperture == 12)
        assert(pdist.scaledAperture == 12 / 20d)

        rap.adjust(1)

        assert(rap.logicalAperture == 13)
        assert(pdist.scaledAperture == 13 / 20d)
      }
  }

  // copied from ApertureTest - can remove test when we remove toggle.
  test("weighted RandomAperture doesn't unduly bias") {
    com.twitter.finagle.toggle.flag.overrides
      .let("com.twitter.finagle.loadbalancer.WeightedAperture.v2", 1.0) {
        val counts = new Counts
        val bal = new Bal {
          override protected val useDeterministicOrdering = Some(false)
        }

        bal.update(counts.range(2))
        assert(bal.aperturex == 1)
        assert(bal.isRandomAperture)

        // last endpoint outside the aperture is open.
        counts(0).status = Status.Busy

        // should be available due to the single endpoint
        assert(bal.status == Status.Open)

        // should be moved forward on rebuild
        val svc = Await.result(bal(ClientConnection.nil))
        assert(bal.rebuilds == 1)
        assert(bal.status == Status.Open)
        assert(svc.status == Status.Open)
      }
  }

  test("discards weights outside the aperture when building pdist") {
    val endpoints = Vector.fill(20)(TestNode(newFactory(equalWeight = true)))

    val wap = new WeightedAperture[Unit, Unit, TestNode](
      aperture = new TestAperture(),
      endpoints = endpoints,
      initAperture = 12,
      coord = FromInstanceId(0, 20)
    )

    assert(
      WeightedAperture
        .adjustWeights(endpoints.map(_.factory.weight), FromInstanceId(0, 20)).length == 20)
    // Aperture size is 12 and we start at index 0, meaning some index above 11 should be out of bounds
    // iff zero-weighted endpoints are dropped
    // Note that values close to 0 are not dropped (ie 1.1102230246251565E-16 still exists)
    // Note also that the aperture size is a measure of distance around the ring, not a specific
    // measure of how many endpoints will exist within that distance.
    intercept[IndexOutOfBoundsException](wap.pdist.get(14))

    // Weights should match
    intercept[IndexOutOfBoundsException](wap.pdist.weight(14))
  }

  test("virtual and real indices are correct") {
    val endpoints = Vector.fill(20)(TestNode(newFactory()))

    /* (0.09081256181340881, 0.014891457880475457, 0.0975219897302887, 0.006559603048899776,
        0.0069517882499888, 0.009075229373741134, 0.042446672815094705, 0.08743910443467884,
        0.057510386410179914, 0.03964968696216305, 0.025831524999190184, 0.015279215278932223,
        0.026042993046009957, 0.0461681947326973, 0.04391007905378173, 0.02114054333698702,
        0.06142121425583968, 0.003345791160297063, 0.06888961212637748, 0.06841127998963015) */

    val wap = new WeightedAperture[Unit, Unit, TestNode](
      aperture = new TestAperture(),
      endpoints = endpoints,
      initAperture = 12,
      coord = FromInstanceId(4, 20)
    )

    // wap.indices.toSeq.sorted:
    // Vector(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18)

    assert(wap.pdist.get(0) == endpoints(2))
    assert(wap.pdist.get(16) == endpoints(18))
  }
}
