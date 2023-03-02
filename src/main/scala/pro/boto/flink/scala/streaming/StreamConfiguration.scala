package pro.boto.flink.scala.streaming

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.configuration.Configuration
import org.apache.flink.configuration.ExecutionOptions
import org.apache.flink.configuration.RestOptions
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig

import scala.concurrent.duration.Duration

@PublicEvolving
class StreamConfiguration {

  private val executionConfig = new ExecutionConfig
  private val checkpointConfig = new CheckpointConfig
  private val configuration = new Configuration

  executionConfig.disableForceAvro()
  executionConfig.disableForceKryo()

  def withParallelism(parallelism: Int): StreamConfiguration = {
    executionConfig.setParallelism(parallelism)
    this
  }

  def withMaxParallelism(parallelism: Int): StreamConfiguration = {
    executionConfig.setMaxParallelism(parallelism)
    this
  }

  def withRestartStrategy(strategy: RestartStrategyConfiguration): StreamConfiguration = {
    executionConfig.setRestartStrategy(strategy)
    this
  }

  def withCheckpointMode(mode: CheckpointingMode): StreamConfiguration = {
    checkpointConfig.setCheckpointingMode(mode)
    this
  }

  def withCheckpointTimeout(timeout: Duration): StreamConfiguration = {
    checkpointConfig.setCheckpointInterval(timeout.toMillis)
    this
  }

  def withRuntimeMode(mode: RuntimeExecutionMode): StreamConfiguration = {
    configuration.set(ExecutionOptions.RUNTIME_MODE, mode)
    this
  }

  def withWebUI(): StreamConfiguration = {
    if (!configuration.contains(RestOptions.PORT)) {
      configuration.setInteger(RestOptions.PORT, RestOptions.PORT.defaultValue)
    }
    this
  }


  protected [streaming] def toJavaConfiguration: Configuration = {
    val fullConfig = new Configuration()
    fullConfig.addAll(configuration)
    fullConfig.addAll(executionConfig.toConfiguration)
    fullConfig.addAll(checkpointConfig.toConfiguration)
    fullConfig
  }

}
