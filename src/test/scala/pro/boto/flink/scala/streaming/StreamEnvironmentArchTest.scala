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

import com.esotericsoftware.kryo.Serializer
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.ArchRule.Assertions
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods
import com.tngtech.archunit.lang.{ArchCondition, ArchRule, ConditionEvents, SimpleConditionEvent}
import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.operators.SlotSharingGroup
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.{ExecutionConfig, JobExecutionResult, RuntimeExecutionMode}
import org.apache.flink.api.connector.source.Source
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.core.execution.{JobClient, JobListener}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.{CheckpointConfig, LocalStreamEnvironment, RemoteStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.{SplittableIterator, TernaryBoolean}
import org.assertj.core.api
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import pro.boto.flink.scala.architecture.RelatedMethod.{classFrom, extendsFrom}
import pro.boto.flink.scala.architecture.{ClassMethods, RelatedMethod}

import java.lang.Long as JavaLong
import java.net.URI
import java.rmi.Remote
import java.util
import java.util.{Collections, List as JavaList}
import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

class StreamEnvironmentArchTest {

  val CREATE_ENV_METHODS: List[RelatedMethod] = List(
    RelatedMethod("createLocalEnvironment", classFrom[LocalStreamEnvironment]),
    RelatedMethod("createLocalEnvironment", classFrom[LocalStreamEnvironment], classFrom[Int]),
    RelatedMethod("createLocalEnvironment", classFrom[LocalStreamEnvironment], classFrom[Configuration]),
    RelatedMethod("createLocalEnvironment", classFrom[LocalStreamEnvironment], classFrom[Int], classFrom[Configuration]),
    RelatedMethod("createLocalEnvironmentWithWebUI", classFrom[StreamExecutionEnvironment], classFrom[Configuration]),
    RelatedMethod("createRemoteEnvironment", classFrom[StreamExecutionEnvironment], classFrom[String], classFrom[Int], classFrom[Array[String]]),
    RelatedMethod("createRemoteEnvironment", classFrom[StreamExecutionEnvironment], classFrom[String], classFrom[Int], classFrom[Int], classFrom[Array[String]]),
    RelatedMethod("createRemoteEnvironment", classFrom[StreamExecutionEnvironment], classFrom[String], classFrom[Int], classFrom[Configuration], classFrom[Array[String]]),
    RelatedMethod("getExecutionEnvironment", classFrom[StreamExecutionEnvironment]),
    RelatedMethod("getExecutionEnvironment", classFrom[StreamExecutionEnvironment], classFrom[Configuration])
  )

  val CHECKPOINT_METHODS: List[RelatedMethod] = List(
    RelatedMethod("getCheckpointConfig", classFrom[CheckpointConfig]),
    RelatedMethod("getCheckpointInterval", classFrom[Long]),
    RelatedMethod("getCheckpointingMode", classFrom[CheckpointingMode]),
    RelatedMethod("enableCheckpointing", classFrom[StreamExecutionEnvironment], classFrom[Long]),
    RelatedMethod("enableCheckpointing", classFrom[StreamExecutionEnvironment], classFrom[Long], classFrom[CheckpointingMode]),
    RelatedMethod("isForceUnalignedCheckpoints", classFrom[Boolean]),
    RelatedMethod("isUnalignedCheckpointsEnabled", classFrom[Boolean])
  )

  val EXECUTION_METHODS: List[RelatedMethod] = List(
    RelatedMethod("getConfig", classFrom[ExecutionConfig]),
    RelatedMethod("setParallelism", classFrom[StreamExecutionEnvironment], classFrom[Int]),
    RelatedMethod("getParallelism", classFrom[Int]),
    RelatedMethod("setMaxParallelism", classFrom[StreamExecutionEnvironment], classFrom[Int]),
    RelatedMethod("getMaxParallelism", classFrom[Int]),
    RelatedMethod("setRestartStrategy", "void", classFrom[RestartStrategyConfiguration]),
    RelatedMethod("getRestartStrategy", classFrom[RestartStrategyConfiguration]),
    RelatedMethod("addDefaultKryoSerializer", "void", classFrom[Class[_]]("?"), "T"),
    RelatedMethod("addDefaultKryoSerializer", "void", classFrom[Class[_]]("?"), classFrom[Class[_]](extendsFrom[Serializer[_]]("?"))),
    RelatedMethod("registerTypeWithKryoSerializer", "void", classFrom[Class[_]]("?"), "T"),
    RelatedMethod("registerTypeWithKryoSerializer", "void", classFrom[Class[_]]("?"), classFrom[Class[_]](extendsFrom[Serializer[_]])),
    RelatedMethod("registerType", "void", classFrom[Class[_]]("?")),
    RelatedMethod("setRuntimeMode", classFrom[StreamExecutionEnvironment], classFrom[RuntimeExecutionMode])
  )

  val JOB_METHODS: List[RelatedMethod] = List(
    RelatedMethod("getJobListeners", classFrom[JavaList[_]](classFrom[JobListener])),
    RelatedMethod("registerJobListener", "void", classFrom[JobListener]),
    RelatedMethod("clearJobListeners", "void"),
    RelatedMethod("getExecutionPlan", classFrom[String]),
    RelatedMethod("execute", classFrom[JobExecutionResult]),
    RelatedMethod("execute", classFrom[JobExecutionResult], classFrom[String]),
    RelatedMethod("executeAsync", classFrom[JobClient]),
    RelatedMethod("executeAsync", classFrom[JobClient], classFrom[String])
  )

  val SOURCE_METHODS: List[RelatedMethod] = List(
    RelatedMethod("addSource", classFrom[DataStreamSource[_]]("OUT"), classFrom[SourceFunction[_]]("OUT")),
    RelatedMethod("addSource", classFrom[DataStreamSource[_]]("OUT"), classFrom[SourceFunction[_]]("OUT"), classFrom[String]),
    RelatedMethod("addSource", classFrom[DataStreamSource[_]]("OUT"), classFrom[SourceFunction[_]]("OUT"), classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("addSource", classFrom[DataStreamSource[_]]("OUT"), classFrom[SourceFunction[_]]("OUT"), classFrom[String], classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("createInput", classFrom[DataStreamSource[_]]("OUT"), classFrom[InputFormat[_, _]]("OUT", "?")),
    RelatedMethod("createInput", classFrom[DataStreamSource[_]]("OUT"), classFrom[InputFormat[_, _]]("OUT", "?"), classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("fromCollection", classFrom[DataStreamSource[_]]("OUT"), classFrom[util.Collection[_]]("OUT")),
    RelatedMethod("fromCollection", classFrom[DataStreamSource[_]]("OUT"), classFrom[util.Collection[_]]("OUT"), classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("fromCollection", classFrom[DataStreamSource[_]]("OUT"), classFrom[util.Iterator[_]]("OUT"), classFrom[Class[_]]("OUT")),
    RelatedMethod("fromCollection", classFrom[DataStreamSource[_]]("OUT"), classFrom[util.Iterator[_]]("OUT"), classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("fromElements", classFrom[DataStreamSource[_]]("OUT"), "OUT[]"),
    RelatedMethod("fromElements", classFrom[DataStreamSource[_]]("OUT"), classFrom[Class[_]]("OUT"), "OUT[]"),
    RelatedMethod("fromParallelCollection", classFrom[DataStreamSource[_]]("OUT"), classFrom[SplittableIterator[_]]("OUT"), classFrom[Class[_]]("OUT")),
    RelatedMethod("fromParallelCollection", classFrom[DataStreamSource[_]]("OUT"), classFrom[SplittableIterator[_]]("OUT"), classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("fromSource", classFrom[DataStreamSource[_]]("OUT"), classFrom[Source[?, ?, ?]]("OUT", "?", "?"), classFrom[WatermarkStrategy[_]]("OUT"), classFrom[String]),
    RelatedMethod("fromSource", classFrom[DataStreamSource[_]]("OUT"), classFrom[Source[?, ?, ?]]("OUT", "?", "?"), classFrom[WatermarkStrategy[_]]("OUT"), classFrom[String], classFrom[TypeInformation[_]]("OUT")),
    RelatedMethod("fromSequence", classFrom[DataStreamSource[_]](classFrom[JavaLong]), classFrom[Long], classFrom[Long]),
    RelatedMethod("socketTextStream", classFrom[DataStreamSource[_]](classFrom[String]), classFrom[String], classFrom[Int]),
    RelatedMethod("socketTextStream", classFrom[DataStreamSource[_]](classFrom[String]), classFrom[String], classFrom[Int], classFrom[String]),
    RelatedMethod("socketTextStream", classFrom[DataStreamSource[_]](classFrom[String]), classFrom[String], classFrom[Int], classFrom[String], classFrom[Long]),
  )

  val STREAM_GRAPH_METHODS: List[RelatedMethod] = List(
    RelatedMethod("setStateBackend", classFrom[StreamExecutionEnvironment], classFrom[StateBackend]),
    RelatedMethod("getStateBackend", classFrom[StateBackend]),
    RelatedMethod("enableChangelogStateBackend", classFrom[StreamExecutionEnvironment], classFrom[Boolean]),
    RelatedMethod("isChangelogStateBackendEnabled", classFrom[TernaryBoolean]),
    RelatedMethod("setDefaultSavepointDirectory", classFrom[StreamExecutionEnvironment], classFrom[String]),
    RelatedMethod("setDefaultSavepointDirectory", classFrom[StreamExecutionEnvironment], classFrom[URI]),
    RelatedMethod("setDefaultSavepointDirectory", classFrom[StreamExecutionEnvironment], classFrom[Path]),
    RelatedMethod("getDefaultSavepointDirectory", classFrom[Path]),
    RelatedMethod("disableOperatorChaining", classFrom[StreamExecutionEnvironment]),
    RelatedMethod("isChainingEnabled", classFrom[Boolean]),
    RelatedMethod("registerCachedFile", "void", classFrom[String], classFrom[String]),
    RelatedMethod("registerCachedFile", "void", classFrom[String], classFrom[String], classFrom[Boolean]),
    RelatedMethod("getCachedFiles", classFrom[JavaList[_]](classFrom[Tuple2[_, _]](classFrom[String], classFrom[DistributedCacheEntry]))),
    RelatedMethod("setBufferTimeout", classFrom[StreamExecutionEnvironment], classFrom[Long]),
    RelatedMethod("getBufferTimeout", classFrom[Long]),
    RelatedMethod("registerSlotSharingGroup", classFrom[StreamExecutionEnvironment], classFrom[SlotSharingGroup]),
  )

  val KNOWN_METHODS: List[RelatedMethod] = List(
    RelatedMethod("getDefaultLocalParallelism", classFrom[Int]),
    RelatedMethod("setDefaultLocalParallelism", "void", classFrom[Int]),
    RelatedMethod("close", "void"),
    RelatedMethod("configure", "void", classFrom[ReadableConfig]),
    RelatedMethod("configure", "void", classFrom[ReadableConfig], classFrom[ClassLoader]),
    ) ++
    CREATE_ENV_METHODS ++
    CHECKPOINT_METHODS ++
    EXECUTION_METHODS ++
    SOURCE_METHODS ++
    STREAM_GRAPH_METHODS ++
    JOB_METHODS

  @Test
  def testArch(): Unit = {
    Assertions.assertNoViolation(ClassMethods.checkClassMethod(classOf[StreamExecutionEnvironment], KNOWN_METHODS))
  }

}