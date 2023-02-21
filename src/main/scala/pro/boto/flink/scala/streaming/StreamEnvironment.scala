/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pro.boto.flink.scala.streaming

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.{ExecutionConfig, JobExecutionResult}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment as JavaEnv}

import java.lang
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

@PublicEvolving
class StreamEnvironment(val javaEnv: JavaEnv) {
  def checkpointConfig: CheckpointConfig = javaEnv.getCheckpointConfig

  def executionConfig: ExecutionConfig = javaEnv.getConfig

  def fromElements[T](data: T*)(implicit classTag: ClassTag[T]): DataStream[T] = {
    new DataStream[T](javaEnv.fromElements(classTag.runtimeClass.asInstanceOf[Class[T]], data:_*))
  }

  def fromCollection[T](data: T*)(implicit typeInformation: TypeInformation[T]): DataStream[T] = {
    fromCollection(data)(typeInformation)
  }

  def fromCollection[T](data: Iterable[T])(implicit typeInformation: TypeInformation[T]): DataStream[T] = {
    new DataStream[T](javaEnv.fromCollection(data.asJavaCollection, typeInformation))
  }

  def fromCollection[T](data: Iterator[T])(implicit typeInformation: TypeInformation[T]): DataStream[T] = {
    new DataStream[T](javaEnv.fromCollection(data.asJava, typeInformation))
  }

  def execute(jobName: String = ""): JobExecutionResult = {
    if (jobName.isBlank) javaEnv.execute()
    else javaEnv.execute(jobName)
  }

  def executeAsync(jobName: String = ""): JobClient = {
    if (jobName.isBlank) javaEnv.executeAsync()
    else javaEnv.executeAsync(jobName)
  }

}

@PublicEvolving
object StreamEnvironment {

  def createEnvironment(configuration: StreamConfiguration = new StreamConfiguration()): StreamEnvironment = {
    new StreamEnvironment(JavaEnv.getExecutionEnvironment(configuration.toJavaConfiguration))
  }

  def createLocalEnvironment(configuration: StreamConfiguration = new StreamConfiguration(), webUI: Boolean = true): StreamEnvironment = {
    val javaEnv =
      if (webUI) {
        JavaEnv.createLocalEnvironmentWithWebUI(configuration.toJavaConfiguration)
      } else {
        JavaEnv.createLocalEnvironment(configuration.toJavaConfiguration)
      }

    new StreamEnvironment(javaEnv)
  }

  def createRemoteEnvironment(host: String, port: Int,
                              configuration: StreamConfiguration = new StreamConfiguration(),
                              jarFiles: String*): StreamEnvironment = {
    new StreamEnvironment(JavaEnv
      .createRemoteEnvironment(host, port, configuration.toJavaConfiguration, jarFiles: _*))
  }
}