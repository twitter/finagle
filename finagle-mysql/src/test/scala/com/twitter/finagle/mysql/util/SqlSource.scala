package com.twitter.finagle.mysql.util

import java.net.URL

/**
 * Loads SQL queries from a source.  The source must format the queries such that
 * they are semicolon separated.  Each query may be multilined, but two queries cannot exist
 * on the same line.  Empty lines and lines starting with '#' character are ignored.
 */
object SqlSource {

  /**
   * Loads SQL queries form a URL.
   *
   * @param url the URL to the SQL content
   *
   * @return an iterator over the queries
   */
  def fromUrl(url: URL): Iterator[String] = {
    parseQueries(io.Source.fromURL(url).getLines()).iterator
  }

  /**
   * Loads SQL queries form a string.
   *
   * @param sql the SQL content
   *
   * @return an iterator over the queries
   */
  def fromString(sql: String): Iterator[String] = {
    parseQueries(io.Source.fromString(sql).getLines()).iterator
  }

  /**
   * Parses the given lines of SQL into queries.  Queries must be newline separated.
   * Each query can multilined, however two queries cannot exist on the same line.
   * Empty lines and lines starting with '#' character are ignored.
   *
   * @param lines the SQL content
   *
   * @return a sequence of queries
   */
  private def parseQueries(lines: Iterator[String]): Seq[String] = {

    val queryAndQueries = lines.foldLeft((List[String](), List[List[String]]())) { (result, line) =>
      val (query, queries) = result
      if (line.isEmpty || line.startsWith("#")) (query, queries)
      else if (line.contains(";")) (List[String](), queries :+ (query :+ line))
      else (query :+ line, queries)
    }

    val queries = if (queryAndQueries._1.isEmpty)
      queryAndQueries._2
    else
      queryAndQueries._2 :+ queryAndQueries._1

    queries map { queryLines =>
      queryLines mkString " "
    }

  }

}
