package pro.boto.flink.scala.streaming

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.util.Collector
import pro.boto.flink.scala.streaming.transformation.async.AsyncTransformation

import scala.jdk.CollectionConverters._

@PublicEvolving
class DataStream[T](val javaStream: JavaStream[T]) extends AsyncTransformation[T] {

  /** Returns the TypeInformation for the elements of this DataStream. */
  def dataType: TypeInformation[T] = javaStream.getType

  def filterNot(function: T => Boolean): DataStream[T] = {
    val filterFunction: FilterFunction[T] = new FilterFunction[T] {
      override def filter(in: T): Boolean = !function(in)
    }
    filter(filterFunction)
  }
  def filter(function: T => Boolean): DataStream[T] = {
    val filterFunction: FilterFunction[T] = new FilterFunction[T] {
      override def filter(in: T): Boolean = function(in)
    }
    filter(filterFunction)
  }

  def filter(function: FilterFunction[T]): DataStream[T] = {
    DataStream(javaStream.filter(function))
  }

  def map[R](function: T => R)(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val mapFunction: MapFunction[T, R] = new MapFunction[T, R] {
      override def map(in: T): R = function(in)
    }
    map(mapFunction)
  }

  def map[R](function: MapFunction[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    DataStream(javaStream.map(function, typeInfo))
  }

  def flatMap[R](function: T => IterableOnce[R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val flatMapFunction: FlatMapFunction[T, R] = new FlatMapFunction[T, R] {
      override def flatMap(in: T, out: Collector[R]): Unit = function(in).iterator.foreach(out.collect)
    }
    flatMap(flatMapFunction)
  }

  def flatMap[R](function: FlatMapFunction[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    DataStream(javaStream.flatMap(function, typeInfo))
  }

  def process[R](function: ProcessFunction[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    DataStream(javaStream.process(function, typeInfo))
  }

  def transform[R](name: String, operator: OneInputStreamOperator[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    DataStream(javaStream.transform(name, typeInfo, operator))
  }

  def executeAndCollect(jobName: String = "DataStreamJob", limit: Int = 0): List[T] = {
    if (limit > 0) {
      javaStream.executeAndCollect(jobName, limit).asScala.toList
    } else {
      javaStream.executeAndCollect(jobName).asScala.toList
    }
  }

  def sinkTo(sink: Sink[T]): DataStreamSink[T] = {
    javaStream.sinkTo(sink)
  }

}

object DataStream {
  def apply[T](javaStream: JavaStream[T]): DataStream[T] = {
    new DataStream[T](javaStream = javaStream)
  }

}