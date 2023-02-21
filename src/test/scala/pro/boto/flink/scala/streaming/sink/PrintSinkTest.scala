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

package pro.boto.flink.scala.streaming.sink

import com.tngtech.archunit.core.domain.*
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.ArchRule.Assertions
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods
import com.tngtech.archunit.lang.{ArchCondition, ArchRule, ConditionEvents, SimpleConditionEvent}
import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.cache.DistributedCache.DistributedCacheEntry
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.{ExecutionConfig, RuntimeExecutionMode}
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, LocalStreamEnvironment, StreamExecutionEnvironmentFactory, StreamExecutionEnvironment as JavaEnv}
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.assertj.core.api
import org.assertj.core.api.Assertions.*
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.wordspec.AnyWordSpec
import pro.boto.flink.scala.architecture.RelatedMethod
import pro.boto.flink.scala.architecture.RelatedMethod.classFrom
import pro.boto.flink.scala.streaming.StreamEnvironment
import pro.boto.flink.scala.streaming.sink.PrintSink.PrintType
import pro.boto.flink.scala.testmocks.{MockSinkInitContext, MockSinkWriterContext}
import pro.boto.flink.scala.typeutils.*

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.Collections
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.util.Using

class PrintSinkTest extends AnyWordSpec {

  private val originalSystemOut: PrintStream = System.out
  private val originalSystemErr: PrintStream = System.err
  private val arrayOutStream: ByteArrayOutputStream = new ByteArrayOutputStream
  private val arrayErrStream: ByteArrayOutputStream = new ByteArrayOutputStream

  private val line = System.lineSeparator

  @BeforeEach
  def beforeEach(): Unit = {
    System.setOut(new PrintStream(arrayOutStream))
    System.setErr(new PrintStream(arrayErrStream))
  }

  @AfterEach
  def afterEach(): Unit = {
    if (System.out ne originalSystemOut) System.out.close()
    if (System.err ne originalSystemErr) System.err.close()
    System.setOut(originalSystemOut)
    System.setErr(originalSystemErr)
  }

  @Test
  def testPrintSinkStdOut(): Unit = {
    val printSink = new PrintSink[String](printType = PrintType.StdOut)
    val writer = printSink.createWriter(new MockSinkInitContext)

    writer.write("hello world!", new MockSinkWriterContext)
    assertThat(arrayOutStream.toString).isEqualTo("hello world!" + line)
    assertThat(arrayErrStream.toString).isEmpty()
  }

  @Test
  def testPrintSinkStdErr(): Unit = {
    val printSink = new PrintSink[String](printType = PrintType.StdErr)
    val writer = printSink.createWriter(new MockSinkInitContext)

    writer.write("hello world!", new MockSinkWriterContext)
    assertThat(arrayOutStream.toString).isEmpty()
    assertThat(arrayErrStream.toString).isEqualTo("hello world!" + line)
  }

}