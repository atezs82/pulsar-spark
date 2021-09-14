package org.apache.spark.sql.pulsar

import scala.util.{Failure, Random, Success, Try}

object RetryUtils {
  def retry[T](f: => T): T = retryRec(10, 500, 0.5, "")(f)

  def retryRec[T](maxRetries: Int,
                  actualBackoff: Int,
                  randomizationFactor: Double,
                  exceptionLog: String)(f: => T): T = {
    Try {
      f
    } match {
      case Success(value) =>
        value
      case Failure(e) =>
        if (maxRetries > 1) {
          Thread.sleep(actualBackoff)
          val nextBackoff = (actualBackoff.toDouble *
            (2 + Random.nextFloat() * randomizationFactor)).toInt
          retryRec(maxRetries - 1,
            nextBackoff,
            randomizationFactor,
            exceptionLog + s"${actualBackoff}(${e.toString})\n")(f)
        } else {
          throw new RuntimeException(s"Failed after backoff. Log of exceptions: $exceptionLog. " +
            "Final exception follows.", e)
        }
    }
  }
}
