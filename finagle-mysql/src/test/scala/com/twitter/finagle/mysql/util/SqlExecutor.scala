package com.twitter.finagle.mysql.util

import com.twitter.finagle.exp.mysql.Client
import com.twitter.util.Future
import java.net.URL

/**
 * Executes SQL.
 */
object SqlExecutor {

  /**
   * Executes SQL queries retrieved from the given URL.  If any query fails, then a failed Future is
   * returned.  The SQL content must be formatted as follows.  SQL queries are terminated by ';'
   * character.  SQL queries may be multilined, but must be delimited by a newline.  Lines starting
   * with '#' or empty lines are skipped.
   *
   * @param client a client
   * @param url a URL to SQL content
   */
  def executeSql(client: Client, url: URL): Future[Unit] = {
    val queries = SqlSource.fromUrl(url)
    val results = (queries map { query =>
      client.query(query)
    }).toSeq
    Future.join(results)
  }

  /**
   * Executes SQL queries from the given URLs.  This method folds the results of calling
   * executeSql(client, url) on each URL.
   *
   * @param client a client
   * @param urls URLs to SQL content
   */
  def executeSql(client: Client, urls: Seq[URL]): Future[Unit] = {
    val results = urls map { url =>
      executeSql(client, url)
    }
    Future.join(results)
  }

}
