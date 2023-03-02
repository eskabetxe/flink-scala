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
import org.junit.jupiter.api.Test
import pro.boto.flink.scala.streaming.StreamEnvironment
import pro.boto.flink.scala.typeutils._

import scala.jdk.CollectionConverters._

class CollectSinkTest {

  @Test
  def testSink(): Unit = {
    val env = StreamEnvironment.createEnvironment()

    val sink = new CollectSink[Int]()
    env.fromCollection(Range(0, 100).iterator)
      .sinkTo(sink)

    env.execute()

    assertThat(sink.values.size).isEqualTo(100)
    assertThat(sink.values.asJava).containsExactlyInAnyOrderElementsOf(Range(0, 100).asJava)

  }

  @Test
  def testSinkIsCleanup(): Unit = {
    // Test again to check if list is cleaned correctly
    testSink()
  }


}