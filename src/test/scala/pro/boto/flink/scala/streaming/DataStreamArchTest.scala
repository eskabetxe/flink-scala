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

import com.tngtech.archunit.lang.ArchRule.Assertions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.operators.ResourceSpec
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.connector.sink
import org.apache.flink.api.connector.sink2
import org.apache.flink.api.java.functions.{KeySelector => JavaKeySelector}
import org.apache.flink.streaming.api.datastream.{DataStream => JavaStream}
import org.apache.flink.streaming.api.datastream.{AllWindowedStream => JavaAllWindowedStream}
import org.apache.flink.streaming.api.datastream.{KeyedStream => JavaKeyedStream}
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.datastream.CoGroupedStreams
import org.apache.flink.streaming.api.datastream.ConnectedStreams
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.datastream.IterativeStream
import org.apache.flink.streaming.api.datastream.JoinedStreams
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.operators.OneInputStreamOperator
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.CloseableIterator
import org.junit.jupiter.api.Test
import pro.boto.flink.scala.architecture.ClassMethods
import pro.boto.flink.scala.architecture.RelatedMethod
import pro.boto.flink.scala.architecture.RelatedMethod.arrayFrom
import pro.boto.flink.scala.architecture.RelatedMethod.classFrom

import java.util.{List => JavaList}

class DataStreamArchTest {

  val ACCESS_METHODS: List[RelatedMethod] = List(
    RelatedMethod("getParallelism", classFrom[Int]),
    RelatedMethod("getExecutionConfig", classFrom[ExecutionConfig]),
    RelatedMethod("getMinResources", classFrom[ResourceSpec]),
    RelatedMethod("getPreferredResources", classFrom[ResourceSpec]),
    RelatedMethod("getType", classFrom[TypeInformation[_]]("T")),
    RelatedMethod("getExecutionEnvironment", classFrom[StreamExecutionEnvironment])
  )

  val TRANSFORMATION_METHODS: List[RelatedMethod] = List(
    RelatedMethod("flatMap", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[FlatMapFunction[_,_]]("T","R")),
    RelatedMethod("flatMap", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[FlatMapFunction[_,_]]("T","R"), classFrom[TypeInformation[_]]("R")),
    RelatedMethod("map", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[MapFunction[_, _]]("T", "R")),
    RelatedMethod("map", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[MapFunction[_, _]]("T", "R"), classFrom[TypeInformation[_]]("R")),
    RelatedMethod("process", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[ProcessFunction[_, _]]("T", "R")),
    RelatedMethod("filter", classFrom[SingleOutputStreamOperator[_]]("T"), classFrom[FilterFunction[_]]("T")),
    RelatedMethod("project", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[Array[Int]]),
    RelatedMethod("transform", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[String], classFrom[TypeInformation[_]]("R"), classFrom[OneInputStreamOperator[_,_]]("T", "R")),
    RelatedMethod("transform", classFrom[SingleOutputStreamOperator[_]]("R"), classFrom[String], classFrom[TypeInformation[_]]("R"), classFrom[OneInputStreamOperatorFactory[_,_]]("T", "R")),
    RelatedMethod("assignTimestampsAndWatermarks", classFrom[SingleOutputStreamOperator[_]]("T"), classFrom[WatermarkStrategy[_]]("T")),
    RelatedMethod("join", classFrom[JoinedStreams[_,_]]("T","T2"), classFrom[JavaStream[_]]("T2")),
    RelatedMethod("coGroup", classFrom[CoGroupedStreams[_, _]]("T", "T2"), classFrom[JavaStream[_]]("T2")),
    RelatedMethod("connect", classFrom[ConnectedStreams[_, _]]("T", "R"), classFrom[JavaStream[_]]("R")),
    RelatedMethod("connect", classFrom[BroadcastConnectedStream[_, _]]("T", "R"), classFrom[BroadcastStream[_]]("R")),
    RelatedMethod("iterate", classFrom[IterativeStream[_]]("T")),
    RelatedMethod("iterate", classFrom[IterativeStream[_]]("T"), classFrom[Long])
  )

  val OPERATIONS_METHODS: List[RelatedMethod] = List(
    RelatedMethod("broadcast", classFrom[JavaStream[_]]("T")),
    RelatedMethod("broadcast", classFrom[BroadcastStream[_]]("T"), arrayFrom[MapStateDescriptor[_,_]]("?", "?")),
    RelatedMethod("forward", classFrom[JavaStream[_]]("T")),
    RelatedMethod("global", classFrom[JavaStream[_]]("T")),
    RelatedMethod("rebalance", classFrom[JavaStream[_]]("T")),
    RelatedMethod("rescale", classFrom[JavaStream[_]]("T")),
    RelatedMethod("shuffle", classFrom[JavaStream[_]]("T")),
    RelatedMethod("partitionCustom", classFrom[JavaStream[_]]("T"), classFrom[Partitioner[_]]("K"), classFrom[JavaKeySelector[_,_]]("T", "K")),
    RelatedMethod("union", classFrom[JavaStream[_]]("T"), arrayFrom[JavaStream[_]]("T")),
    RelatedMethod("keyBy", classFrom[JavaKeyedStream[_,_]]("T","K"), classFrom[JavaKeySelector[_,_]]("T","K")),
    RelatedMethod("keyBy", classFrom[JavaKeyedStream[_,_]]("T","K"), classFrom[JavaKeySelector[_,_]]("T","K"), classFrom[TypeInformation[_]]("K")),
    RelatedMethod("windowAll", classFrom[JavaAllWindowedStream[_,_]]("T", "W"), classFrom[WindowAssigner[_,_]]("? super T", "W")),
    RelatedMethod("countWindowAll", classFrom[JavaAllWindowedStream[_,_]]("T", classFrom[GlobalWindow]), classFrom[Long]),
    RelatedMethod("countWindowAll", classFrom[JavaAllWindowedStream[_,_]]("T", classFrom[GlobalWindow]), classFrom[Long], classFrom[Long])
  )

  val SINK_METHODS: List[RelatedMethod] = List(
    RelatedMethod("addSink", classFrom[DataStreamSink[_]]("T"), classFrom[SinkFunction[_]]("T")),
    RelatedMethod("print", classFrom[DataStreamSink[_]]("T")),
    RelatedMethod("print", classFrom[DataStreamSink[_]]("T"), classFrom[String]),
    RelatedMethod("printToErr", classFrom[DataStreamSink[_]]("T")),
    RelatedMethod("printToErr", classFrom[DataStreamSink[_]]("T"), classFrom[String]),
    RelatedMethod("sinkTo", classFrom[DataStreamSink[_]]("T"), classFrom[sink.Sink[_,_,_,_]]("T", "?", "?", "?")),
    RelatedMethod("sinkTo", classFrom[DataStreamSink[_]]("T"), classFrom[sink.Sink[_,_,_,_]]("T", "?", "?", "?"), classFrom[CustomSinkOperatorUidHashes]),
    RelatedMethod("sinkTo", classFrom[DataStreamSink[_]]("T"), classFrom[sink2.Sink[_]]("T")),
    RelatedMethod("sinkTo", classFrom[DataStreamSink[_]]("T"), classFrom[sink2.Sink[_]]("T"), classFrom[CustomSinkOperatorUidHashes]),
    RelatedMethod("writeToSocket", classFrom[DataStreamSink[_]]("T"), classFrom[String], classFrom[Int], classFrom[SerializationSchema[_]]("T")),
    RelatedMethod("executeAndCollect", classFrom[CloseableIterator[_]]("T")),
    RelatedMethod("executeAndCollect", classFrom[CloseableIterator[_]]("T"), classFrom[String]),
    RelatedMethod("executeAndCollect", classFrom[JavaList[_]]("T"), classFrom[Int]),
    RelatedMethod("executeAndCollect", classFrom[JavaList[_]]("T"), classFrom[String], classFrom[Int])
  )

  val KNOWN_METHODS: List[RelatedMethod] =
    ACCESS_METHODS ++
    TRANSFORMATION_METHODS ++
    OPERATIONS_METHODS ++
    SINK_METHODS

  @Test
  def testArch(): Unit = {
    Assertions.assertNoViolation(ClassMethods.checkKnownMethod(classOf[JavaStream[_]], KNOWN_METHODS))
    Assertions.assertNoViolation(ClassMethods.checkKnownMethodExist(classOf[JavaStream[_]], KNOWN_METHODS))
    Assertions.assertNoViolation(ClassMethods.checkClassMethod(classOf[JavaStream[_]], KNOWN_METHODS))
  }

}