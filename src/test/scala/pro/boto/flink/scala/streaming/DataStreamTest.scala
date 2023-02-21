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

import com.tngtech.archunit.core.domain.*
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.ArchRule.Assertions
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods
import com.tngtech.archunit.lang.{ArchCondition, ArchRule, ConditionEvents, SimpleConditionEvent}
import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.{ExecutionConfig, RuntimeExecutionMode}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, LocalStreamEnvironment, StreamExecutionEnvironmentFactory, StreamExecutionEnvironment as JavaEnv}
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.assertj.core.api
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.Test
import pro.boto.flink.scala.architecture.RelatedMethod.classFrom
import pro.boto.flink.scala.architecture.{ClassMethods, RelatedMethod}
import pro.boto.flink.scala.typeutils.*

import java.util.Collections
import scala.jdk.CollectionConverters.*

class DataStreamTest {

  @Test
  def testFromElements(): Unit = {
    val env = StreamEnvironment.createEnvironment()
    val stream = env.fromElements(1L, 100L)
    assertThat(stream.executeAndCollect()).isEqualTo(List(1L, 100L))
  }

  @Test
  def testFromCollection(): Unit = {
    val env = StreamEnvironment.createEnvironment()
    val stream = env.fromCollection(1L, 100L)
    assertThat(stream.executeAndCollect()).isEqualTo(List(1L, 100L))
  }

  @Test
  def testFromCollectionWithJava(): Unit = {
    val env = StreamEnvironment.createEnvironment()
    val stream = env.fromCollection(java.lang.Long.parseLong("1"), java.lang.Long.parseLong("100"))
    assertThat(stream.executeAndCollect()).isEqualTo(List(1L, 100L))
  }

}