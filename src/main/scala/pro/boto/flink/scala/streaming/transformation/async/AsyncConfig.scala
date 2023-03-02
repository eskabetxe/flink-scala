package pro.boto.flink.scala.streaming.transformation.async

import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies
import pro.boto.flink.scala.streaming.transformation.async.AsyncConfig.AsyncOrder.AsyncOrder

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

case class AsyncConfig[R](order: AsyncOrder,
                          timeout: Duration = 30.seconds,
                          capacity: Int = 100,
                          retryStrategy: AsyncRetryStrategy[R] = AsyncRetryStrategies.NO_RETRY_STRATEGY.asInstanceOf[AsyncRetryStrategy[R]])
extends Serializable

object AsyncConfig {
  object AsyncOrder extends Enumeration {
    type AsyncOrder = Value
    val Ordered, Unordered = Value
  }

}
