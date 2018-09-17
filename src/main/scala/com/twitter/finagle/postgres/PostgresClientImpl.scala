package com.twitter.finagle.postgres

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.cache.Refresh
import com.twitter.conversions.time._
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values._
import com.twitter.finagle.{Service, ServiceFactory, Status}
import com.twitter.logging.Logger
import com.twitter.util._
import org.jboss.netty.buffer.ChannelBuffer

import scala.language.{existentials, implicitConversions}
import scala.util.Random

/*
 * A Finagle client for communicating with Postgres.
 */


class PostgresClientImpl(
  factory: ServiceFactory[PgRequest, PgResponse],
  id:String,
  types: Option[Map[Int, PostgresClient.TypeSpecifier]] = None,
  receiveFunctions: PartialFunction[String, ValueDecoder[T] forSome {type T}],
  binaryResults: Boolean = false,
  binaryParams: Boolean = false
) extends PostgresClient {
  private[this] val counter = new AtomicInteger(0)
  private[this] val logger = Logger(getClass.getName)
  private val resultFormats = if(binaryResults) Seq(1) else Seq(0)
  private val paramFormats = if(binaryParams) Seq(1) else Seq(0)

  val charset = StandardCharsets.UTF_8

  private def retrieveTypeMap() = {
    //get a mapping of OIDs to the name of the receive function for all types in the remote DB.
    //typreceive is the most reliable way to determine how a type should be decoded
    val customTypesQuery =
    """
      |SELECT DISTINCT
      |  CAST(t.typname AS text) AS type,
      |  CAST(t.oid AS integer) AS oid,
      |  CAST(t.typreceive AS text) AS typreceive,
      |  CAST(t.typelem AS integer) AS typelem
      |FROM           pg_type t
      |WHERE          CAST(t.typreceive AS text) <> '-'
    """.stripMargin

    val serviceF = factory.apply

    val bootstrapTypes = Map(
      Types.INT_4 -> ValueDecoder.int4,
      Types.TEXT -> ValueDecoder.string
    )

    val customTypesResult = for {
      service <- serviceF
      response <- service.apply(PgRequest(Query(customTypesQuery)))
    } yield response match {
      case SelectResult(fields, rows) =>
        val rowValues = ResultSet(fields, charset, rows, PostgresClient.defaultTypes, receiveFunctions).rows
        rowValues.map {
          row => row.get[Int]("oid") -> PostgresClient.TypeSpecifier(
            row.get[String]("typreceive"),
            row.get[String]("type"),
            row.get[Int]("typelem"))
        }.toMap
    }

    customTypesResult.ensure {
      serviceF.foreach(_.close())
    }

    customTypesResult
  }


  private[postgres] val typeMap = Refresh.every(1.hour) {
    types.map(Future(_)).getOrElse(retrieveTypeMap())
  }

  // The OIDs to be used when sending parameters
  private[postgres] val encodeOids =
    typeMap().map {
      tm =>
        tm.toIndexedSeq.map {
          case (oid, PostgresClient.TypeSpecifier(receiveFn, typeName, elemOid)) => typeName -> oid
        }.groupBy(_._1).mapValues(_.map(_._2).min)
    }

  /*
   * Execute some actions inside of a transaction using a single connection
   */
  override def inTransaction[T](fn: PostgresClient => Future[T]): Future[T] = for {
    types                    <- typeMap()
    service                  <- factory()
    constFactory             =  ServiceFactory.const(service)
    id                       =  Random.alphanumeric.take(28).mkString
    transactionalClient      =  new PostgresClientImpl(constFactory, id, Some(types), receiveFunctions, binaryResults, binaryParams)
    closeTransaction         =  () => transactionalClient.close().ensure(constFactory.close().ensure(service.close()))
    completeTransactionQuery =  (sql: String) => transactionalClient.query(sql).ensure(closeTransaction())
    _                        <- transactionalClient.query("BEGIN").onFailure(_ => closeTransaction())
    result                   <- fn(transactionalClient).rescue {
      case err => for {
        _ <- completeTransactionQuery("ROLLBACK")
        _ <- Future.exception(err)
      } yield null.asInstanceOf[T]
    }
    _                        <- completeTransactionQuery("COMMIT")
  } yield result

  /*
   * Issue an arbitrary SQL query and get the response.
   */
  override def query(sql: String): Future[QueryResponse] = sendQuery(sql) {
    case SelectResult(fields, rows) => typeMap().map {
      types => ResultSet(fields, charset, rows, types, receiveFunctions)
    }
    case CommandCompleteResponse(affected) => Future(OK(affected))
  }

  /*
   * Issue a single SELECT query and get the response.
   */
  override def fetch(sql: String): Future[SelectResult] = sendQuery(sql) {
    case rs: SelectResult => Future(rs)
  }

  /*
   * Execute an update command (e.g., INSERT, DELETE) and get the response.
   */
  override def executeUpdate(sql: String): Future[OK] = sendQuery(sql) {
    case CommandCompleteResponse(rows) => Future(OK(rows))
  }

  override def execute(sql: String): Future[OK] = executeUpdate(sql)

  /*
   * Run a single SELECT query and wrap the results with the provided function.
   */
  override def select[T](sql: String)(f: Row => T): Future[Seq[T]] = for {
    types  <- typeMap()
    result <- fetch(sql)
  } yield result match {
    case SelectResult(fields, rows) => ResultSet(fields, charset, rows, types, receiveFunctions).rows.map(f)
  }

  /*
   * Issue a single, prepared SELECT query and wrap the response rows with the provided function.
   */
  override def prepareAndQuery[T](sql: String, params: Param[_]*)(f: Row => T): Future[Seq[T]] = {
    typeMap().flatMap { _ =>
      for {
        service   <- factory()
        statement = new PreparedStatementImpl("", sql, service)
        result    <- statement.select(params: _*)(f)
      } yield result
    }
  }

  /*
   * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
   */
  override def prepareAndExecute(sql: String, params: Param[_]*): Future[Int] = {
    typeMap().flatMap { _ =>
      for {
        service   <- factory()
        statement = new PreparedStatementImpl("", sql, service)
        OK(count) <- statement.exec(params: _*)
      } yield count
    }
  }


  /**
    * Close the underlying connection pool and make this Client eternally down
    * @return
    */
  override def close(): Future[Unit] = {
    factory.close()
  }

  /**
   * The current availability [[Status]] of this client.
   */
  override def status: Status = factory.status

  /**
   * Determines whether this client is available (can accept requests
   * with a reasonable likelihood of success).
   */
  override def isAvailable: Boolean = status == Status.Open

  private[this] def sendQuery[T](sql: String)(handler: PartialFunction[PgResponse, Future[T]]) = {
    send(PgRequest(Query(sql)))(handler)
  }


  private[this] def send[T](
    r: PgRequest,
    optionalService: Option[Service[PgRequest, PgResponse]] = None)(
    handler: PartialFunction[PgResponse, Future[T]]
  ) = {
    val service = optionalService.getOrElse(factory.toService)
    service(r).flatMap (handler orElse {
      case unexpected => Future.exception(new IllegalStateException(s"Unexpected response $unexpected"))
    })
  }


  private[this] class PreparedStatementImpl(
    name: String,
    sql: String,
    service: Service[PgRequest, PgResponse]
  ) extends PreparedStatement {

    def closeService = service.close()

    private[this] def parse(params: Param[_]*): Future[Unit] = {
      val paramTypes = encodeOids.map {
        oidMap => params.map {
          param => oidMap.getOrElse(param.encoder.typeName, 0)
        }
      }
      paramTypes.flatMap {
        types =>
          val req = Parse(name, sql, types)
          send(PgRequest(req, flush = true), Some(service)) {
            case ParseCompletedResponse => Future.value(())
          }
      }
    }

    private[this] def bind(params: Seq[ChannelBuffer]): Future[Unit] = {
      val req =  Bind(
        portal = name,
        name = name,
        formats = paramFormats,
        params = params,
        resultFormats = resultFormats
      )

      send(PgRequest(req, flush = true), Some(service)) {
        case BindCompletedResponse => Future.value(())
      }
    }

    private[this] def describe(): Future[Array[Field]] = {
      val req = PgRequest(Describe(portal = true, name = name), flush = true)
      send(req, Some(service)) {
        case RowDescriptions(fields) => Future.value(fields)
      }
    }


    private[this] def execute(
      maxRows: Int = 0
    ) = {
      val req = PgRequest(Execute(name, maxRows), flush = true)
      send(req, Some(service)) {
        case rep => Future.value(rep)
      }
    }


    private[this] def sync(
      optionalService: Option[Service[PgRequest, PgResponse]] = None
    ): Future[Unit] = send(PgRequest(Sync), optionalService) {
      case ReadyForQueryResponse => Future.value(())
    }


    override def fire(params: Param[_]*): Future[QueryResponse] = {
      val paramBuffers = if(binaryParams) {
        params.map {
          p => p.encodeBinary(StandardCharsets.UTF_8)
        }
      } else {
        params.map {
          p => p.encodeText(StandardCharsets.UTF_8)
        }
      }

      val f = for {
        types  <- typeMap()
        pname  <- parse(params: _*)
        _      <- bind(paramBuffers)
        fields <- describe()
        exec   <- execute()
      } yield exec match {
        case CommandCompleteResponse(rows) => OK(rows)
        case Rows(rows, true) => ResultSet(fields, charset, rows, types, receiveFunctions)
      }
      f.transform {
        result =>
          sync(Some(service)).flatMap {
            _ => Future.const(result)
          }
      }.ensure(service.close())
    }
  }

  private[this] def genName() = s"fin-pg-$id-" + counter.incrementAndGet
}




case class Param[T](value: T)(implicit val encoder: ValueEncoder[T]) {
  def encodeText(charset: Charset = StandardCharsets.UTF_8) = ValueEncoder.encodeText(value, encoder, charset)
  def encodeBinary(charset: Charset = StandardCharsets.UTF_8) = ValueEncoder.encodeBinary(value, encoder, charset)
}

object Param {
  implicit def convert[T : ValueEncoder](t: T): Param[T] = Param(t)
}
