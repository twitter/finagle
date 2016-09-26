package com.twitter.finagle.postgres

import java.nio.charset.{Charset, StandardCharsets}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.immutable.Queue
import scala.util.Random

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.builder.{ClientBuilder, ClientConfig}
import com.twitter.finagle.postgres.codec.{ClientError, Errors, PgCodec, ServerError}
import com.twitter.finagle.postgres.messages._
import com.twitter.finagle.postgres.values._
import com.twitter.logging.Logger
import com.twitter.util._
import org.jboss.netty.buffer.ChannelBuffer
import scala.language.implicitConversions

import com.twitter.finagle.service._


/*
 * A Finagle client for communicating with Postgres.
 */
class Client(
  factory: ServiceFactory[PgRequest, PgResponse],
  id:String,
  types: Option[Map[Int, Client.TypeSpecifier]] = None,
  customReceiveFunctions: PartialFunction[String, ValueDecoder[T] forSome {type T}] = { case "noop" => ValueDecoder.Unknown },
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
      Type.INT_4 -> ValueDecoder.Int4,
      Type.TEXT -> ValueDecoder.String
    )

    val customTypesResult = for {
      service <- serviceF
      response <- service.apply(PgRequest(Query(customTypesQuery)))
    } yield response match {
      case SelectResult(fields, rows) =>
        val rowValues = rows.map {
          row =>
            row.data.zip(fields).map {
              case (buf, field) =>
                val decoder = bootstrapTypes(field.dataType)
                if(field.format == 0)
                  decoder.decodeText(Buffers.readString(buf, charset)).getOrElse(null)
                else
                  decoder.decodeBinary(buf, charset).getOrElse(null)
            }
        }
        val fieldNames = fields.map(_.name)
        rowValues.map(row => new Row(fieldNames, row)).map {
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

  private[postgres] val decoders = customReceiveFunctions orElse ValueDecoder.decoders


  /*
   * Execute some actions inside of a transaction using a single connection
   */
  def inTransaction[T](fn: Client => Future[T]) = for {
    service             <- factory()
    constFactory        = ServiceFactory.const(service)
    transactionalClient = new Client(constFactory, Random.alphanumeric.take(28).mkString)
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
    case SelectResult(fields, rows) => processFields(fields).map {
      case (names, parsers) => ResultSet(names, charset, parsers, rows)
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
  def select[T](sql: String)(f: Row => T): Future[Seq[T]] = fetch(sql).flatMap {
    rs =>
      extractRows(rs).map(_.map(f))
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
    send(PgRequest(new Query(sql)))(handler)
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
    ): Future[(IndexedSeq[String], IndexedSeq[((ChannelBuffer, Charset)) => Try[Value[T]] forSome {type T}])] =
    send(PgRequest(Describe(portal = true, name = name), flush = true), optionalService) {
      case RowDescriptions(fields) => processFields(fields)
    }


  private[this] def execute(
      name: String,
      maxRows: Int = 0,
      optionalService: Option[Service[PgRequest, PgResponse]] = None) = {
    fire(PgRequest(Execute(name, maxRows), flush = true), optionalService)
  }

  private[this] def sync(
      optionalService: Option[Service[PgRequest, PgResponse]] = None): Future[Unit] = {
    send(PgRequest(Sync), optionalService) {
      case ReadyForQueryResponse => Future.value(())
    }
  }

  private[this] def fire(r: PgRequest, optionalService: Option[Service[PgRequest, PgResponse]] = None) = {
    optionalService match {
      case Some(service) =>
        // A service has been passed in; use it
        service.apply(r)
      case _ =>
        // Create a new service instance from the client factory
        factory.toService(r)
    }
  }

  private[this] def send[T](
      r: PgRequest,
      optionalService: Option[Service[PgRequest, PgResponse]] = None
    )(handler: PartialFunction[PgResponse, Future[T]]) = {
    fire(r, optionalService) flatMap (handler orElse {
      case some => throw new UnsupportedOperationException("TODO Support exceptions correctly " + some)
    })
  }

  private[this] def processFields(
      fields: IndexedSeq[Field]): Future[(IndexedSeq[String], IndexedSeq[((ChannelBuffer, Charset)) => Try[Value[T]] forSome {type T}])] = {
    val names = fields.map(f => f.name)
    val parsers = fields.toList.map {
      f => for {
        types <- typeMap
      } yield for {
        Client.TypeSpecifier(recv, name, elem) <- types.get(f.dataType)
        decoder <- decoders.lift.apply(recv)
      } yield if(f.format != 0) (decoder.decodeBinary _).tupled else (Buffers.readString _).tupled.andThen(decoder.decodeText)
    }.foldLeft(Future(Queue.empty[((ChannelBuffer, Charset)) => Try[Value[T]] forSome {type T}])) {
      (accumF, next) => accumF flatMap {
        accum => next map {
          d => accum.enqueue(d.getOrElse(ValueDecoder.unknownBinary _))
        }
      }
    }

    parsers.map {
      decoders => (names, decoders.toIndexedSeq)
    }
  }

  private[this] def extractRows(rs: SelectResult): Future[List[Row]] = processFields(rs.fields) map {
    case (fieldNames, fieldParsers) =>

    rs.rows.map(dataRow => new Row(fieldNames, dataRow.data.zip(fieldParsers).map {
      case (d, p) => if (d == null) null else p(d, charset).getOrElse(null)
    }))
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
        _ <- bind(name, paramBuffers, Some(service))
        (fieldNames, fieldParsers) <- describe(name, Some(service))
        exec <- execute(name, optionalService = Some(service))
      } yield exec match {
          case CommandCompleteResponse(rows) => OK(rows)
          case Rows(rows, true) => ResultSet(fieldNames, charset, fieldParsers, rows)
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

  private[postgres] val defaultTypes = Map(
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

  def apply(
    host: String,
    username: String,
    password: Option[String],
    database: String,
    useSsl: Boolean = false,
    hostConnectionLimit: Int = 1,
    retryPolicy: RetryPolicy[Try[Nothing]] = RetryPolicy.backoff(
      Backoff.exponential(Duration.fromMilliseconds(50), 2, Duration.fromSeconds(5)))(
      RetryPolicy.TimeoutAndWriteExceptionsOnly orElse RetryPolicy.ChannelClosedExceptionsOnly),
    customTypes: Boolean = false,
    customReceiveFunctions: PartialFunction[String, ValueDecoder[T] forSome {type T}] = { case "noop" => ValueDecoder.Unknown },
    binaryResults: Boolean = false,
    binaryParams: Boolean = false
  ): Client = {
    val id = Random.alphanumeric.take(28).mkString

    // Classify responses appropriately - a ServerError with SQLState or ClientError does not mean that the client is
    // down.
    val classifier: ResponseClassifier = {
      case ReqRep(a, Return(_)) => ResponseClass.Success
      case ReqRep(a, Throw(ServerError(_, _, _, Some(_), _, _, _))) => ResponseClass.Success
      case ReqRep(a, Throw(ClientError(_))) => ResponseClass.Success
    }

    withBuilder(
      host,
      username,
      password,
      database,
      useSsl,
      customTypes,
      customReceiveFunctions,
      binaryResults,
      binaryParams) {
      cb =>
        cb.hostConnectionLimit(hostConnectionLimit)
          .responseClassifier(classifier)
          .retryPolicy(retryPolicy)
    }
  }

  def withBuilder(
    host: String,
    username: String,
    password: Option[String],
    database: String,
    useSsl: Boolean = false,
    customTypes: Boolean = false,
    customReceiveFunctions: PartialFunction[String, ValueDecoder[T] forSome {type T}] = { case "noop" => ValueDecoder.Unknown },
    binaryResults: Boolean = false,
    binaryParams: Boolean = false)(
    builderF: ClientBuilder[PgRequest, PgResponse, ClientConfig.Yes, ClientConfig.Yes, Nothing] =>
      ClientBuilder[PgRequest, PgResponse, ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes]
  ) = {
    val id = Random.alphanumeric.take(28).mkString
    val builder = builderF(
      ClientBuilder()
        .codec(new PgCodec(username, password, database, id, useSsl = useSsl))
        .hosts(host)
    )
    val types = if(!customTypes) Some(defaultTypes) else None
    new Client(builder.buildFactory(), id, types, customReceiveFunctions, binaryResults, binaryParams)
  }
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