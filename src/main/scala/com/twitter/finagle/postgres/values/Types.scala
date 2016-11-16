package com.twitter.finagle.postgres.values

/*
 * Enumeration of value types that can be included in query results.
 */
object Types {
  val BOOL = 16
  val BYTE_A = 17
  val CHAR = 18
  val NAME = 19
  val INT_8 = 20
  val INT_2 = 21
  val INT_4 = 23
  val REG_PROC = 24
  val TEXT = 25
  val OID = 26
  val TID = 27
  val XID = 28
  val CID = 29
  val XML = 142
  val POINT = 600
  val L_SEG = 601
  val PATH = 602
  val BOX = 603
  val POLYGON = 604
  val LINE = 628
  val CIDR = 650
  val FLOAT_4 = 700
  val FLOAT_8 = 701
  val ABS_TIME = 702
  val REL_TIME = 703
  val T_INTERVAL = 704
  val UNKNOWN = 705
  val CIRCLE = 718
  val MONEY = 790
  val MAC_ADDR = 829
  val INET = 869
  val BP_CHAR = 1042
  val VAR_CHAR = 1043
  val DATE = 1082
  val TIME = 1083
  val TIMESTAMP = 1114
  val TIMESTAMP_TZ = 1184
  val INTERVAL = 1186
  val TIME_TZ = 1266
  val BIT = 1560
  val VAR_BIT = 1562
  val NUMERIC = 1700
  val REF_CURSOR = 1790
  val RECORD = 2249
  val VOID = 2278
  val UUID = 2950
}
