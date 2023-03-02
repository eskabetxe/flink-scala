package pro.boto.flink.scala.streaming.sink

import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import pro.boto.flink.scala.streaming.sink.PrintSink.PrintType
import pro.boto.flink.scala.streaming.sink.PrintSink.PrintType.PrintType
import pro.boto.flink.scala.streaming.sink.PrintSink.PrintWriter

import java.io.IOException


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
    new PrintWriter(printType, prefix)
  }
}

object PrintSink {
  object PrintType extends Enumeration {
    type PrintType = Value
    val StdOut, StdErr, LogDebug, LogInfo, LogWarn, LogError, LogTrace = Value
  }

  class PrintWriter[T](printType: PrintType, identifier: String) extends SinkWriter[T] with Serializable {
    private val LOG: Logger = LoggerFactory.getLogger(this.getClass)

    @throws[IOException]
    @throws[InterruptedException]
    override def write(input: T, context: SinkWriter.Context): Unit = {
      val inputString = s"$identifier${input.toString}"
      printType match {
        case PrintType.StdOut => System.out.println(inputString)
        case PrintType.StdErr => System.err.println(inputString)
        case PrintType.LogDebug => LOG.debug(inputString)
        case PrintType.LogInfo => LOG.info(inputString)
        case PrintType.LogWarn => LOG.warn(inputString)
        case PrintType.LogError => LOG.error(inputString)
        case PrintType.LogTrace => LOG.trace(inputString)
      }
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

