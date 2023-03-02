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

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.concurrent.Executors
import org.assertj.core.api.Assertions._
import org.junit.jupiter.api.Test
import pro.boto.flink.scala.streaming.transformation.async.AsyncConfig
import pro.boto.flink.scala.streaming.transformation.async.AsyncConfig.AsyncOrder
import pro.boto.flink.scala.streaming.DataStreamTest.delayAndDoubleEvenNumbers
import pro.boto.flink.scala.testutils.cluster.WithFlinkCluster
import pro.boto.flink.scala.typeutils._

import java.util.Collections
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

@WithFlinkCluster
class DataStreamTest {
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

  @Test
  def testFromElements(): Unit = {
    val result = StreamEnvironment
      .createEnvironment()
      .fromElements(1L, 100L)
      .executeAndCollect()

    assertThat(result).isEqualTo(List(1L, 100L))
  }

  @Test
  def testFromElementsWithJava(): Unit = {
    val result = StreamEnvironment
      .createEnvironment()
      .fromElements(java.lang.Long.parseLong("1"), java.lang.Long.parseLong("100"))
      .executeAndCollect()

    assertThat(result).isEqualTo(List(1L, 100L))
  }

  @Test
  def testFromCollection(): Unit = {
    val range = (1L to 10L).toList
    val result = StreamEnvironment
      .createEnvironment()
      .fromCollection(range)
      .executeAndCollect()

    assertThat(result).isEqualTo(range)
  }

  @Test
  def testFilter(): Unit = {
    val result = StreamEnvironment
      .createEnvironment()
      .fromCollection(1L to 10L)
      .filter(new FilterFunction[Long] {
        override def filter(value: Long): Boolean = value > 2
      })
      .filter((value: Long) => value < 8)
      .filter(_ % 2 == 0)
      .filterNot(_ == 4)
      .executeAndCollect()

    assertThat(result.asJava).containsOnly(6L)
  }

  @Test
  def testMap(): Unit = {
    val result = StreamEnvironment
      .createEnvironment()
      .fromElements(1L, 10L)
      .map(new MapFunction[Long, Long] {
        override def map(value: Long): Long = value + 1
      })
      .map((value: Long) => value + 2)
      .map(_ + 3)
      .executeAndCollect()

    assertThat(result.asJava)
      .containsExactlyInAnyOrderElementsOf(List(7L, 16L).asJava)
  }

  @Test
  def testFlatMap(): Unit = {
    val result = StreamEnvironment
      .createEnvironment()
      .fromCollection(1L to 20L)
      .flatMap(new FlatMapFunction[Long, Long] {
        override def flatMap(value: Long, collector: Collector[Long]): Unit = {
          if (value % 2 != 0) collector.collect(value)
        }
      })
      .flatMap((value: Long, collector: Collector[Long]) => if (value < 10) collector.collect(value * 2))
      .flatMap(x => if (x % 2 == 0) Option(x) else None)
      .executeAndCollect()

    assertThat(result.asJava)
      .containsExactlyInAnyOrderElementsOf(List(2L, 6L, 10L, 14L, 18L).asJava)
  }

  @Test
  def testProcess(): Unit = {
    val result = StreamEnvironment
      .createEnvironment()
      .fromCollection(1L to 10)
      .process(new ProcessFunction[Long,Long] {
        override def processElement(value: Long, context: ProcessFunction[Long, Long]#Context, collector: Collector[Long]): Unit = {
          if (value % 2 != 0) collector.collect(value * 2)
        }
      })
      .executeAndCollect()

    assertThat(result.asJava)
      .containsExactlyInAnyOrder(List(2L, 6L, 10L, 14, 18L):_*)
  }

  @Test
  def testAsyncOrdered(): Unit = {
    val range: List[Long] = (1L to 10L).toList
    val streamConfig = new StreamConfiguration().withParallelism(1)
    val asyncConfig = AsyncConfig[Long](AsyncOrder.Ordered)

    val result = StreamEnvironment
      .createEnvironment(streamConfig)
      .fromCollection(range)
      .async(config = asyncConfig,
        function = new RichAsyncFunction[Long, Long] {
        override def asyncInvoke(in: Long, resultFuture: ResultFuture[Long]): Unit = {
          resultFuture.complete(List(in).asJavaCollection)
        }
      })
      .async(config = asyncConfig,
             function = delayAndDoubleEvenNumbers)
      .executeAndCollect()

    assertThat(result.asJava)
      .containsExactly(1L, 4L, 3L, 8L, 5L, 12L, 7L, 16L, 9L, 20L)
  }

  @Test
  def testAsyncUnordered(): Unit = {
    val range: List[Long] = (1L to 10L).toList
    val asyncConfig = AsyncConfig[Long](AsyncOrder.Unordered)
    val result = StreamEnvironment
      .createEnvironment()
      .fromCollection(range)
      .async(config = asyncConfig,
        function = new RichAsyncFunction[Long, Long] {
        override def asyncInvoke(in: Long, resultFuture: ResultFuture[Long]): Unit = {
          if (in % 2 == 0) Thread.sleep(30)
          resultFuture.complete(Collections.singletonList(in))
        }
      })
      .async(config = asyncConfig,
        function = delayAndDoubleEvenNumbers)
      .executeAndCollect()

    assertThat(result.slice(0, 5).asJava)
      .containsExactlyInAnyOrder(List(1L, 3L, 5L, 7L, 9L): _*)

    assertThat(result.slice(5, 10).asJava)
      .containsExactlyInAnyOrder(List(4L, 8L, 12L, 16L, 20L): _*)
  }

}

object DataStreamTest {
  implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
  val delayAndDoubleEvenNumbers: Long => Future[Long] = (in: Long) => {
    if (in % 2 == 0) {
      Thread.sleep(30)
      Future(in * 2)
    } else {
      Future(in)
    }

  }

}
