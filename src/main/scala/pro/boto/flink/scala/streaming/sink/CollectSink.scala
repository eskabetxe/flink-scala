package pro.boto.flink.scala.streaming.sink

import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import pro.boto.flink.scala.streaming.sink.CollectSink.CollectWriter

import java.io.IOException
import java.util.Collections
import scala.jdk.CollectionConverters._


class CollectSink[T <: Any]() extends Sink[T] with Serializable {

  clearValues()

  @throws[IOException]
  override def createWriter(initContext: Sink.InitContext): SinkWriter[T] = CollectWriter.asInstanceOf[SinkWriter[T]]

  def values: List[T] = {
    CollectWriter.values.asScala.toList.asInstanceOf[List[T]]
  }

  def clearValues(): Unit = {
    CollectWriter.values.clear()
  }
}

object CollectSink {

  object CollectWriter extends SinkWriter[Any] with Serializable {
    val values: java.util.List[Any] = Collections.synchronizedList(new java.util.ArrayList())

    @throws[IOException]
    @throws[InterruptedException]
    override def write(input: Any, context: SinkWriter.Context): Unit = {
      values.add(input)
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def flush(flush: Boolean): Unit = {

    }

    @throws[Exception]
    def close(): Unit = {

    }
  }
}

