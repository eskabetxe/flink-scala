package pro.boto.flink.scala.testutils.mocks

import org.apache.flink.api.connector.sink2.SinkWriter

import java.lang

class MockSinkWriterContext extends SinkWriter.Context {
  override def currentWatermark(): Long = 0

  override def timestamp(): lang.Long = System.currentTimeMillis()
}
