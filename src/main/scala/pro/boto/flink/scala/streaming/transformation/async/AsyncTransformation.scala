package pro.boto.flink.scala.streaming.transformation.async

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies
import org.apache.flink.util.concurrent.Executors
import pro.boto.flink.scala.streaming.DataStream
import pro.boto.flink.scala.streaming.transformation.async.AsyncConfig.AsyncOrder

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import AsyncTransformation._

import scala.jdk.FutureConverters.FutureOps
abstract class AsyncTransformation[T] {
  def javaStream: JavaStream[T]
  def async[R](function: T => Future[R], config: AsyncConfig[R])
              (implicit typeInfo: TypeInformation[R]): DataStream[R] = {

    val asyncFunction: RichAsyncFunction[T, R] = new RichAsyncFunction[T, R] with Serializable {

      override def asyncInvoke(in: T, result: ResultFuture[R]): Unit = {
//        function(in).onComplete {
//          case Failure(exception) => result.completeExceptionally(exception)
//          case Success(value) => result.complete(List(value).asJavaCollection)
//        }
        function(in).asJava.whenComplete { (response, error) =>
          Option(response) match {
            case Some(r) => result.complete(List(r).asJavaCollection)
            case None => result.completeExceptionally(error)
          }
        }
      }
    }
    async(asyncFunction, config)
  }

  def async[R](function: RichAsyncFunction[T, R], config: AsyncConfig[R])
              (implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val asyncResult = config.order match {
      case AsyncOrder.Ordered => AsyncDataStream
        .orderedWaitWithRetry(javaStream, function, config.timeout.toMillis, TimeUnit.MILLISECONDS, config.capacity, config.retryStrategy)
      case AsyncOrder.Unordered => AsyncDataStream
        .unorderedWaitWithRetry(javaStream, function, config.timeout.toMillis, TimeUnit.MILLISECONDS, config.capacity, config.retryStrategy)
    }
    DataStream(asyncResult.returns(typeInfo))
  }

}

object AsyncTransformation {
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
}
