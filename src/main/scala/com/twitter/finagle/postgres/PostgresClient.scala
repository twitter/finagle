package com.twitter.finagle.postgres
import java.nio.charset.Charset

import com.twitter.finagle.postgres.messages.SelectResult
import com.twitter.finagle.postgres.values.Types
import com.twitter.util.Future

trait PostgresClient {

  def charset: Charset

  /*
   * Execute some actions inside of a transaction using a single connection
   */
  def inTransaction[T](fn: PostgresClient => Future[T]): Future[T]

  /*
   * Issue an arbitrary SQL query and get the response.
   */
  def query(
    sql: String
  ): Future[QueryResponse]

  /*
   * Issue a single SELECT query and get the response.
   */
  def fetch(
    sql: String
  ): Future[SelectResult]

  /*
   * Execute an update command (e.g., INSERT, DELETE) and get the response.
   */
  def executeUpdate(
    sql: String
  ): Future[OK]

  def execute(sql: String): Future[OK]

  /*
   * Run a single SELECT query and wrap the results with the provided function.
   */
  def select[T](sql: String)
    (f: Row => T): Future[Seq[T]]

  /*
   * Issue a single, prepared SELECT query and wrap the response rows with the provided function.
   */
  def prepareAndQuery[T](sql: String, params: Param[_]*)
    (f: Row => T): Future[Seq[T]]

  /*
   * Issue a single, prepared arbitrary query without an expected result set, and provide the affected row count
   */
  def prepareAndExecute(
    sql: String, params: Param[_]*
  ): Future[Int]

  /**
    * Close the underlying connection pool and make this Client eternally down
    *
    * @return
    */
  def close(): Future[Unit]
}


object PostgresClient {

  case class TypeSpecifier(receiveFunction: String, typeName: String, elemOid: Long = 0)

  private[finagle] val defaultTypes = Map(
    Types.BOOL -> TypeSpecifier("boolrecv", "bool"),
    Types.BYTE_A -> TypeSpecifier("bytearecv", "bytea"),
    Types.CHAR -> TypeSpecifier("charrecv", "char"),
    Types.NAME -> TypeSpecifier("namerecv", "name"),
    Types.INT_8 -> TypeSpecifier("int8recv", "int8"),
    Types.INT_2 -> TypeSpecifier("int2recv", "int2"),
    Types.INT_4 -> TypeSpecifier("int4recv", "int4"),
    Types.REG_PROC -> TypeSpecifier("regprocrecv", "regproc"),
    Types.TEXT -> TypeSpecifier("textrecv", "text"),
    Types.OID -> TypeSpecifier("oidrecv", "oid"),
    Types.TID -> TypeSpecifier("tidrecv", "tid"),
    Types.XID -> TypeSpecifier("xidrecv", "xid"),
    Types.CID -> TypeSpecifier("cidrecv", "cid"),
    Types.XML -> TypeSpecifier("xml_recv", "xml"),
    Types.POINT -> TypeSpecifier("point_recv", "point"),
    Types.L_SEG -> TypeSpecifier("lseg_recv", "lseg"),
    Types.PATH -> TypeSpecifier("path_recv", "path"),
    Types.BOX -> TypeSpecifier("box_recv", "box"),
    Types.POLYGON -> TypeSpecifier("poly_recv", "poly"),
    Types.LINE -> TypeSpecifier("line_recv", "line"),
    Types.CIDR -> TypeSpecifier("cidr_recv", "cidr"),
    Types.FLOAT_4 -> TypeSpecifier("float4recv", "float4"),
    Types.FLOAT_8 -> TypeSpecifier("float8recv", "float8"),
    Types.ABS_TIME -> TypeSpecifier("abstimerecv", "abstime"),
    Types.REL_TIME -> TypeSpecifier("reltimerecv", "reltime"),
    Types.T_INTERVAL -> TypeSpecifier("tinternalrecv", "tinternal"),
    Types.UNKNOWN -> TypeSpecifier("unknownrecv", "unknown"),
    Types.CIRCLE -> TypeSpecifier("circle_recv", "circle"),
    Types.MONEY -> TypeSpecifier("cash_recv", "cash"),
    Types.MAC_ADDR -> TypeSpecifier("macaddr_recv", "macaddr"),
    Types.INET -> TypeSpecifier("inet_recv", "inet"),
    Types.BP_CHAR -> TypeSpecifier("bpcharrecv", "bpchar"),
    Types.VAR_CHAR -> TypeSpecifier("varcharrecv", "varchar"),
    Types.DATE -> TypeSpecifier("date_recv", "date"),
    Types.TIME -> TypeSpecifier("time_recv", "time"),
    Types.TIMESTAMP -> TypeSpecifier("timestamp_recv", "timestamp"),
    Types.TIMESTAMP_TZ -> TypeSpecifier("timestamptz_recv", "timestamptz"),
    Types.INTERVAL -> TypeSpecifier("interval_recv", "interval"),
    Types.TIME_TZ -> TypeSpecifier("timetz_recv", "timetz"),
    Types.BIT -> TypeSpecifier("bit_recv", "bit"),
    Types.VAR_BIT -> TypeSpecifier("varbit_recv", "varbit"),
    Types.NUMERIC -> TypeSpecifier("numeric_recv", "numeric"),
    Types.RECORD -> TypeSpecifier("record_recv", "record"),
    Types.VOID -> TypeSpecifier("void_recv", "void"),
    Types.UUID -> TypeSpecifier("uuid_recv", "uuid")
  )

}
