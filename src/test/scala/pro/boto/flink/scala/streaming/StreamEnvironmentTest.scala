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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaEnv}
import org.assertj.core.api.Assertions._
import org.junit.jupiter.api.Test

class StreamEnvironmentTest {
  private def createJavaEnvironment(): JavaEnv = {
    val expected = JavaEnv.getExecutionEnvironment()
    expected.getConfig.disableForceKryo()
    expected.getConfig.disableForceAvro()
    expected
  }

  @Test
  def testCreateEnvironment(): Unit = {
    val env = StreamEnvironment.createEnvironment()
    val expected = createJavaEnvironment()

    assertThat(env.executionConfig).isEqualTo(expected.getConfig)
    assertThat(env.checkpointConfig.toConfiguration).isEqualTo(expected.getCheckpointConfig.toConfiguration)
  }

  @Test
  def testCreateLocalEnvironment(): Unit = {
    val env = StreamEnvironment.createLocalEnvironment()
    val expected = createJavaEnvironment()

    assertThat(env.executionConfig).isEqualTo(expected.getConfig)
    assertThat(env.checkpointConfig.toConfiguration).isEqualTo(expected.getCheckpointConfig.toConfiguration)
  }

  @Test
  def testCreateRemoteEnvironment(): Unit = {
    val env = StreamEnvironment.createRemoteEnvironment(host = "localhost", port = 8081)
    val expected = createJavaEnvironment()

    assertThat(env.executionConfig).isEqualTo(expected.getConfig)
    assertThat(env.checkpointConfig.toConfiguration).isEqualTo(expected.getCheckpointConfig.toConfiguration)
  }

  @Test
  def testCheckpointConfig(): Unit = {
    val env = StreamEnvironment.createEnvironment()
    env.checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.checkpointConfig.setCheckpointInterval(1000)

    val expected = createJavaEnvironment()
    expected.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE)

    assertThat(env.executionConfig).isEqualTo(expected.getConfig)
  }

  /** Verifies that calls to fromSequence() instantiate a new DataStream that contains a sequence of numbers. */
  @Test
  def testFromElements(): Unit = {
    val env    = StreamEnvironment.createEnvironment()
    val stream = env.fromElements(1L, 100L)

    assertThat(stream.dataType).isEqualTo(TypeInformation.of(classOf[Long]))
  }


}