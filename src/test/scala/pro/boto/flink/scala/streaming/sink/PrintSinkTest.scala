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

import org.assertj.core.api.Assertions._
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import pro.boto.flink.scala.streaming.sink.PrintSink.PrintType
import pro.boto.flink.scala.testutils.mocks.MockSinkInitContext
import pro.boto.flink.scala.testutils.mocks.MockSinkWriterContext

import java.io.ByteArrayOutputStream
import java.io.PrintStream

class PrintSinkTest {

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