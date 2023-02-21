package pro.boto.flink.scala.streaming

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.sink2
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.streaming.api.datastream.{BroadcastStream, DataStreamSink, SingleOutputStreamOperator, AllWindowedStream as JavaAllWindowedStream, DataStream as JavaStream, KeyedStream as JavaKeyedStream}


import scala.io.StdIn
import scala.jdk.CollectionConverters.*

@PublicEvolving
class DataStream[T](val javaStream: JavaStream[T]) {

  /** Returns the TypeInformation for the elements of this DataStream. */
  def dataType: TypeInformation[T] = javaStream.getType

  def map[R](function: MapFunction[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    new DataStream(javaStream.map(function, typeInfo))
  }

  def flatMap[R](function: FlatMapFunction[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    javaStream.flatMap(function, typeInfo)
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

  private implicit def apply[T](javaStream: JavaStream[T]): DataStream[T] = {
    new DataStream[T](javaStream = javaStream)
  }
}