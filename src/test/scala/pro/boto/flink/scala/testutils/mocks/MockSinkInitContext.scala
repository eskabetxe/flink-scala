package pro.boto.flink.scala.testutils.mocks

import org.apache.flink.api.common.operators.MailboxExecutor
import org.apache.flink.api.common.operators.ProcessingTimeService
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.metrics.groups.SinkWriterMetricGroup
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService
import org.apache.flink.util.SimpleUserCodeClassLoader
import org.apache.flink.util.UserCodeClassLoader
import org.apache.flink.util.function.ThrowingRunnable

import java.util.OptionalLong

class MockSinkInitContext(numSubtasks: Int = 1, subtaskId: Int = 0) extends Sink.InitContext with SerializationSchema.InitializationContext {
  override def getMailboxExecutor: MailboxExecutor = new MailboxExecutor {
    override def execute(throwingRunnable: ThrowingRunnable[_ <: Exception], s: String, objects: Any*): Unit = {}
    override def `yield`(): Unit = {}
    override def tryYield(): Boolean = false
  }

  override def getProcessingTimeService: ProcessingTimeService = new TestProcessingTimeService

  override def getSubtaskId: Int = subtaskId

  override def getNumberOfParallelSubtasks: Int = numSubtasks

  override def getAttemptNumber: Int = 0

  override def metricGroup(): SinkWriterMetricGroup = InternalSinkWriterMetricGroup.mock(new UnregisteredMetricsGroup)

  override def getRestoredCheckpointId: OptionalLong = OptionalLong.empty()

  override def asSerializationSchemaInitializationContext(): SerializationSchema.InitializationContext = this

  override def getMetricGroup: MetricGroup = metricGroup()

  override def getUserCodeClassLoader: UserCodeClassLoader = SimpleUserCodeClassLoader.create(this.getClass.getClassLoader);
}
