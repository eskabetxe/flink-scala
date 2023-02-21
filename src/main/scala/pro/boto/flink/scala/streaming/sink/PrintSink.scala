package pro.boto.flink.scala.streaming.sink

import org.apache.flink.api.common.eventtime.Watermark
import org.apache.flink.api.connector.sink2.{Sink, SinkWriter}
import org.slf4j.{Logger, LoggerFactory}
import pro.boto.flink.scala.streaming.sink.PrintSink.{PrintType, PrintWriter}

import java.io.IOException
import java.util.{ArrayList, Collections}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*


class PrintSink[T](printType: PrintType = PrintType.StdOut, identifier: String = "") extends Sink[T] with Serializable {
  @throws[IOException]
  override def createWriter(initContext: Sink.InitContext): SinkWriter[T] = {
    val subtaskIndex: Int = initContext.getSubtaskId
    val numParallelSubtasks: Int = initContext.getNumberOfParallelSubtasks
    var prefix: String = identifier
    if (numParallelSubtasks > 1) {
      if (prefix.nonEmpty) prefix += ": "
      prefix += s"Writer (${subtaskIndex + 1}/${numParallelSubtasks})"
    }
    if (prefix.nonEmpty) prefix += " -> "
    PrintWriter(printType, prefix)
  }
}

object PrintSink {
  enum PrintType {
    case StdOut, StdErr, LogDebug, LogInfo, LogWarn, LogError, LogTrace
  }

  class PrintWriter[T](printType: PrintType, identifier: String) extends SinkWriter[T] with Serializable {
    val LOG: Logger = LoggerFactory.getLogger(this.getClass)

    @throws[IOException]
    @throws[InterruptedException]
    override def write(input: T, context: SinkWriter.Context): Unit = {
      val inputString = s"$identifier${input.toString}"
      printType match
        case PrintType.StdOut => System.out.println(inputString)
        case PrintType.StdErr => System.err.println(inputString)
        case PrintType.LogDebug => LOG.debug(inputString)
        case PrintType.LogInfo => LOG.info(inputString)
        case PrintType.LogWarn => LOG.warn(inputString)
        case PrintType.LogError => LOG.error(inputString)
        case PrintType.LogTrace => LOG.trace(inputString)
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

