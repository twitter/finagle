namespace java com.twitter.finagle.thrift

struct TranscriptRecord {
  1: i32 host;
  2: i64 timestamp_ms;
  3: string message;
}

struct Tracing {
  1: i32 transaction_id;
  2: bool debug;
  3: list<TranscriptRecord> transcript;
}
