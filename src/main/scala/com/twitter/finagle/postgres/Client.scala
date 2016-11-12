package com.twitter.finagle.postgres

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.Queue
import scala.language.implicitConversions
import scala.util.Random

import com.twitter.finagle.postgres.codec.Errors
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values._
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.logging.Logger
import com.twitter.util._
import org.jboss.netty.buffer.ChannelBuffer

import scala.language.existentials

/*
 * A Finagle client for communicating with Postgres.
 */
class Client(
  factory: ServiceFactory[PgRequest, PgResponse],
  id:String,
  types: Option[Map[Int, Client.TypeSpecifier]] = None,
  receiveFunctions: PartialFunction[String, ValueDecoder[T] forSome {type T}],
  binaryResults: Boolean = false,
  binaryParams: Boolean = false
) {
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
      Type.INT_4 -> ValueDecoder.int4,
      Type.TEXT -> ValueDecoder.string
    )

    val customTypesResult = for {
      service <- serviceF
      response <- service.apply(PgRequest(Query(customTypesQuery)))
    } yield response match {
      case SelectResult(fields, rows) =>
        val rowValues = ResultSet(fields, charset, rows, Client.defaultTypes, receiveFunctions).rows
        rowValues.map {
          row => row.get[Int]("oid") -> Client.TypeSpecifier(
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

  private[postgres] val typeMap = types.map(Future(_)).getOrElse(retrieveTypeMap())

  // The OIDs to be used when sending parameters
  private[postgres] val encodeOids = typeMap.map {
    tm => tm.toIndexedSeq.map {
      case (oid, Client.TypeSpecifier(receiveFn, typeName, elemOid)) => typeName -> oid
    }.groupBy(_._1).mapValues(_.map(_._2).min)
  }

  /*
   * Execute some actions inside of a transaction using a single connection
   */
  def inTransaction[T](fn: Client => Future[T]) = for {
    types               <- typeMap
    service             <- factory()
    constFactory        =  ServiceFactory.const(service)
    id                  =  Random.alphanumeric.take(28).mkString
    transactionalClient = new Client(constFactory, id, Some(types), receiveFunctions, binaryResults, binaryParams)
    _                   <- transactionalClient.query("BEGIN")
    result              <- fn(transactionalClient).rescue {
                          case err => for {
                            _ <- transactionalClient.query("ROLLBACK")
                            _ <- constFactory.close()
                            _ <- service.close()
                            _ <- Future.exception(err)
                          } yield null.asInstanceOf[T]
                        }
    _                   <- transactionalClient.query("COMMIT")
    _                   <- constFactory.close()
    _                   <- service.close()
  } yield result

  /*
   * Issue an arbitrary SQL query and get the response.
   */
  def query(sql: String): Future[QueryResponse] = sendQuery(sql) {
    case SelectResult(fields, rows) => typeMap.map {
      types => ResultSet(fields, charset, rows, types, receiveFunctions)
    }
    case CommandCompleteResponse(affected) => Future(OK(affected))
  }

  /*
   * Issue a single SELECT query and get the response.
   */
  def fetch(sql: String): Future[SelectResult] = sendQuery(sql) {
    case rs: SelectResult => Future(rs)
  }

  /*
   * Execute an update command (e.g., INSERT, DELETE) and get the response.
   */
  def executeUpdate(sql: String): Future[OK] = sendQuery(sql) {
    case CommandCompleteResponse(rows) => Future(OK(rows))
  }

  /*
   * Run a single SELECT query and wrap the results with the provided function.
   */
  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = for {
    types  <- typeMap
    result <- fetch(sql)
  } yield result match {
    case SelectResult(fields, rows) => ResultSet(fields, charset, rows, types, receiveFunctions).rows.map(f)
  }

  /*
   * Issue a single, prepared SELECT query and wrap the response rows with the provided function.
   */
  def prepareAndQuery[T](sql: String, params: Param[_]*)(f: Row => T): Future[Seq[T]] = {
    val preparedStatement = factory.apply().flatMap {
      service =>
        parse(sql, Some(service), params: _*).map { name =>
          new PreparedStatementImpl(name, service)
        }.rescue {
          case err => sync(Some(service)).flatMap {
            _ =>
              service.close().flatMap {
                _ => Future.exception(err)
              }
          }
        }
    }

    preparedStatement.flatMap {
      statement =>
        statement.select(params: _*)(f).ensure {
          statement.closeService
        }
    }
  }

  /*
   * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
   */
  def prepareAndExecute(sql: String, params: Param[_]*):Future[Int] = {
    val preparedStatement = factory.apply().flatMap {
      service =>
        parse(sql, Some(service), params: _*).map { name =>
          new PreparedStatementImpl(name, service)
        }.rescue {
          case err => sync(Some(service)).flatMap {
            _ =>
              service.close().flatMap {
                _ => Future.exception(err)
              }
          }
        }
    }

    preparedStatement.flatMap {
      statement =>
        statement.exec(params: _*).ensure {
          statement.closeService
        }
    } map {
      case OK(count) => count
    }
  }

  /**
    * Close the underlying connection pool and make this Client eternally down
    * @return
    */
  def close(): Future[Unit] = {
    factory.close()
  }

  private[this] def sendQuery[T](sql: String)(handler: PartialFunction[PgResponse, Future[T]]) = {
    send(PgRequest(Query(sql)))(handler)
  }

  private[this] def parse(
      sql: String,
      optionalService: Option[Service[PgRequest, PgResponse]],
      params: Param[_]*): Future[String] = {
    val name = genName()

    val paramTypes = encodeOids.map {
      oidMap => params.map {
        param => oidMap.getOrElse(param.encoder.typeName, 0)
      }
    }

    paramTypes.flatMap {
      types =>
        val req = Parse(name, sql, types)
        send(PgRequest(req, flush = true), optionalService) {
          case ParseCompletedResponse => Future.value(name)
        }
    }
  }

  private[this] def bind(
      name: String,
      params: Seq[ChannelBuffer] = Seq(),
      optionalService: Option[Service[PgRequest, PgResponse]] = None): Future[Unit] = {

    val req =  Bind(
      portal = name,
      name = name,
      formats = paramFormats,
      params = params,
      resultFormats = resultFormats
    )

    send(PgRequest(req, flush = true), optionalService) {
      case BindCompletedResponse => Future.value(())
    }
  }

  private[this] def describe(
    name: String,
    optionalService: Option[Service[PgRequest, PgResponse]] = None
  ): Future[Array[Field]] = send(PgRequest(Describe(portal = true, name = name), flush = true), optionalService) {
    case RowDescriptions(fields) => Future.value(fields)
  }


  private[this] def execute(
    name: String,
    maxRows: Int = 0,
    optionalService: Option[Service[PgRequest, PgResponse]] = None
  ) = send(PgRequest(Execute(name, maxRows), flush = true), optionalService){ case rep => Future.value(rep) }


  private[this] def sync(
      optionalService: Option[Service[PgRequest, PgResponse]] = None
  ): Future[Unit] = send(PgRequest(Sync), optionalService) {
    case ReadyForQueryResponse => Future.value(())
  }

  private[this] def send[T](
    r: PgRequest,
    optionalService: Option[Service[PgRequest, PgResponse]] = None)(
    handler: PartialFunction[PgResponse, Future[T]]
  ) = {
    val service = optionalService.getOrElse(factory.toService)
    service(r).flatMap (handler orElse {
      case unexpected => Future.exception(new IllegalStateException(s"Unexpected response $unexpected"))
    }).onFailure {
      err => service.close()
    }
  }


  private[this] class PreparedStatementImpl(
      name: String,
      service: Service[PgRequest, PgResponse]) extends PreparedStatement {
    def closeService = service.close()

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
        types  <- typeMap
        _      <- bind(name, paramBuffers, Some(service))
        fields <- describe(name, Some(service))
        exec   <- execute(name, optionalService = Some(service))
      } yield exec match {
          case CommandCompleteResponse(rows) => OK(rows)
          case Rows(rows, true) => ResultSet(fields, charset, rows, types, receiveFunctions)
        }
      f transform {
        result =>
          sync(Some(service)).flatMap {
            _ => Future.const(result)
          }
      }
    }
  }

  private[this] def genName() = s"fin-pg-$id-" + counter.incrementAndGet
}

/*
 * Helper companion object that generates a client from authentication information.
 */
object Client {

  case class TypeSpecifier(receiveFunction: String, typeName: String, elemOid: Long = 0)

  private[finagle] val defaultTypes = Map(
    Type.BOOL -> TypeSpecifier("boolrecv", "bool"),
    Type.BYTE_A -> TypeSpecifier("bytearecv", "bytea"),
    Type.CHAR -> TypeSpecifier("charrecv", "char"),
    Type.NAME -> TypeSpecifier("namerecv", "name"),
    Type.INT_8 -> TypeSpecifier("int8recv", "int8"),
    Type.INT_2 -> TypeSpecifier("int2recv", "int2"),
    Type.INT_4 -> TypeSpecifier("int4recv", "int4"),
    Type.REG_PROC -> TypeSpecifier("regprocrecv", "regproc"),
    Type.TEXT -> TypeSpecifier("textrecv", "text"),
    Type.OID -> TypeSpecifier("oidrecv", "oid"),
    Type.TID -> TypeSpecifier("tidrecv", "tid"),
    Type.XID -> TypeSpecifier("xidrecv", "xid"),
    Type.CID -> TypeSpecifier("cidrecv", "cid"),
    Type.XML -> TypeSpecifier("xml_recv", "xml"),
    Type.POINT -> TypeSpecifier("point_recv", "point"),
    Type.L_SEG -> TypeSpecifier("lseg_recv", "lseg"),
    Type.PATH -> TypeSpecifier("path_recv", "path"),
    Type.BOX -> TypeSpecifier("box_recv", "box"),
    Type.POLYGON -> TypeSpecifier("poly_recv", "poly"),
    Type.LINE -> TypeSpecifier("line_recv", "line"),
    Type.CIDR -> TypeSpecifier("cidr_recv", "cidr"),
    Type.FLOAT_4 -> TypeSpecifier("float4recv", "float4"),
    Type.FLOAT_8 -> TypeSpecifier("float8recv", "float8"),
    Type.ABS_TIME -> TypeSpecifier("abstimerecv", "abstime"),
    Type.REL_TIME -> TypeSpecifier("reltimerecv", "reltime"),
    Type.T_INTERVAL -> TypeSpecifier("tinternalrecv", "tinternal"),
    Type.UNKNOWN -> TypeSpecifier("unknownrecv", "unknown"),
    Type.CIRCLE -> TypeSpecifier("circle_recv", "circle"),
    Type.MONEY -> TypeSpecifier("cash_recv", "cash"),
    Type.MAC_ADDR -> TypeSpecifier("macaddr_recv", "macaddr"),
    Type.INET -> TypeSpecifier("inet_recv", "inet"),
    Type.BP_CHAR -> TypeSpecifier("bpcharrecv", "bpchar"),
    Type.VAR_CHAR -> TypeSpecifier("varcharrecv", "varchar"),
    Type.DATE -> TypeSpecifier("date_recv", "date"),
    Type.TIME -> TypeSpecifier("time_recv", "time"),
    Type.TIMESTAMP -> TypeSpecifier("timestamp_recv", "timestamp"),
    Type.TIMESTAMP_TZ -> TypeSpecifier("timestamptz_recv", "timestamptz"),
    Type.INTERVAL -> TypeSpecifier("interval_recv", "interval"),
    Type.TIME_TZ -> TypeSpecifier("timetz_recv", "timetz"),
    Type.BIT -> TypeSpecifier("bit_recv", "bit"),
    Type.VAR_BIT -> TypeSpecifier("varbit_recv", "varbit"),
    Type.NUMERIC -> TypeSpecifier("numeric_recv", "numeric"),
    Type.RECORD -> TypeSpecifier("record_recv", "record"),
    Type.VOID -> TypeSpecifier("void_recv", "void"),
    Type.UUID -> TypeSpecifier("uuid_recv", "uuid")
  )

}

/*
 * A query that supports parameter substitution. Can help prevent SQL injection attacks.
 */
trait PreparedStatement {
  def fire(params: Param[_]*): Future[QueryResponse]

  def exec(params: Param[_]*): Future[OK] = fire(params: _*) map {
    case ok: OK => ok
    case ResultSet(_) => throw Errors.client("Update query expected")
  }

  def select[T](params: Param[_]*)(f: Row => T): Future[Seq[T]] = fire(params: _*) map {
    case ResultSet(rows) => rows.map(f)
    case OK(_) => Seq.empty[Row].map(f)
  }

  def selectFirst[T](params: Param[_]*)(f: Row => T): Future[Option[T]] =
    select[T](params:_*)(f) flatMap { rows => Future.value(rows.headOption) }
}

case class Param[T](value: T)(implicit val encoder: ValueEncoder[T]) {
  def encodeText(charset: Charset = StandardCharsets.UTF_8) = ValueEncoder.encodeText(value, encoder, charset)
  def encodeBinary(charset: Charset = StandardCharsets.UTF_8) = ValueEncoder.encodeBinary(value, encoder, charset)
}

object Param {
  implicit def convert[T : ValueEncoder](t: T): Param[T] = Param(t)
}