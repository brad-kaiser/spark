/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag

import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import org.apache.spark.rpc._
import org.apache.spark.storage.{BlockId, BlockManagerId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages.{GetCachedBlocks, GetMemoryStatus, GetSizeOfBlocks, ReplicateOneBlock}

class CacheRecoveryManagerSuite extends SparkFunSuite with MockitoSugar with Matchers {
  val oneGB = 1024L * 1024L * 1024L * 1024L
  val plentyOfMem = Map(BlockManagerId("1", "host", 12, None) -> ((oneGB, oneGB)),
                        BlockManagerId("2", "host", 12, None) -> ((oneGB, oneGB)),
                        BlockManagerId("3", "host", 12, None) -> ((oneGB, oneGB)))

  test("GracefulShutdown will take blocks until empty and then kill executor") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(2, 1))
    val bmme = FakeBMM(1, blocks.iterator, plentyOfMem)
    val bmmeRef = DummyRef(bmme)
    val crms = new CacheRecoveryManagerState(bmmeRef, eam, conf)
    val cacheRecoveryManager = new CacheRecoveryManager(crms, conf)

    when(eam.killExecutors(Seq("1"))).thenReturn(Seq("1"))

    cacheRecoveryManager.startCacheRecovery(Seq("1"))
    Thread.sleep(1000)
    verify(eam).killExecutors(Seq("1"))
    bmme.replicated.asScala.toSeq shouldBe blocks
  }

  test("GracefulShutdown will kill executor if it takes too long to replicate") {
    val conf = new SparkConf().set("spark.dynamicAllocation.cacheRecovery.timeout", "1s")
    val eam = mock[ExecutorAllocationManager]
    val blocks = Set(RDDBlockId(1, 1), RDDBlockId(2, 1), RDDBlockId(3, 1), RDDBlockId(4, 1))
    val bmme = FakeBMM(600, blocks.iterator, plentyOfMem)
    val bmmeRef = DummyRef(bmme)
    val crms = new CacheRecoveryManagerState(bmmeRef, eam, conf)
    val cacheRecoveryManager = new CacheRecoveryManager(crms, conf)

    cacheRecoveryManager.startCacheRecovery(Seq("1"))
    Thread.sleep(1010)
    verify(eam, times(1)).killExecutors(Seq("1"), forceIfPending = true)
    bmme.replicated.size shouldBe 1
  }

  test("shutdown timer will get cancelled if replication finishes") {
    val conf = new SparkConf().set("spark.dynamicAllocation.cacheRecovery.timeout", "1s")
    val eam = mock[ExecutorAllocationManager]
    val blocks = Set(RDDBlockId(1, 1))
    val bmme = FakeBMM(1, blocks.iterator, plentyOfMem)
    val bmmeRef = DummyRef(bmme)
    val crms = new CacheRecoveryManagerState(bmmeRef, eam, conf)
    val cacheRecoveryManager = new CacheRecoveryManager(crms, conf)

    cacheRecoveryManager.startCacheRecovery(Seq("1"))
    Thread.sleep(1100)
    // should be killed once not twice
    verify(eam, times(1)).killExecutors(Seq("1"), forceIfPending = true)
  }

  test("Blocks don't get replicated more than once") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1))
    val bmme = FakeBMM(1, blocks.iterator, plentyOfMem)
    val bmmeRef = DummyRef(bmme)
    val crms = new CacheRecoveryManagerState(bmmeRef, eam, conf)
    val cacheRecoveryManager = new CacheRecoveryManager(crms, conf)

    cacheRecoveryManager.startCacheRecovery(Seq("1"))
    Thread.sleep(100)
    bmme.replicated.size shouldBe 1
    bmme.replicated.asScala.toSeq shouldBe Seq(RDDBlockId(1, 1))
  }

  test("Blocks won't replicate if we are running out of space") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1))
    val memStatus = Map(BlockManagerId("1", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("2", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("3", "host", 12, None) -> ((2L, 1L)),
      BlockManagerId("4", "host", 12, None) -> ((3L, 3L)))
    val bmme = FakeBMM(1, blocks.iterator, memStatus)
    val bmmeRef = DummyRef(bmme)
    val crms = new CacheRecoveryManagerState(bmmeRef, eam, conf)
    val cacheRecoveryManager = new CacheRecoveryManager(crms, conf)

    cacheRecoveryManager.startCacheRecovery(Seq("1", "2", "3"))
    Thread.sleep(100)
    bmme.replicated.size shouldBe  2
    bmme.replicated.asScala.toSeq shouldBe Seq(RDDBlockId(1, 1), RDDBlockId(1, 1))
  }

  test("Blocks won't replicate if we are stopping all executors") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1))
    val memStatus = Map(BlockManagerId("1", "host", 12, None) -> ((2L, 1L)),
                         BlockManagerId("2", "host", 12, None) -> ((2L, 1L)),
                         BlockManagerId("3", "host", 12, None) -> ((2L, 1L)),
                         BlockManagerId("4", "host", 12, None) -> ((2L, 1L)))
    val bmme = FakeBMM(1, blocks.iterator, memStatus)
    val bmmeRef = DummyRef(bmme)
    val crms = new CacheRecoveryManagerState(bmmeRef, eam, conf)
    val cacheRecoveryManager = new CacheRecoveryManager(crms, conf)

    cacheRecoveryManager.startCacheRecovery(Seq("1", "2", "3", "4"))
    Thread.sleep(100)
    bmme.replicated.size shouldBe 0
    bmme.replicated.asScala.toSeq shouldBe Seq()
  }
}

private case class FakeBMM(
    pauseMillis: Int,
    blocks: Iterator[BlockId],
    memStatus: Map[BlockManagerId, (Long, Long)],
    sizeOfBlock: Long = 1
  ) extends ThreadSafeRpcEndpoint {

  val rpcEnv = null
  val replicated = new ConcurrentLinkedQueue[BlockId]()

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCachedBlocks(_) =>
      val result: collection.Set[BlockId] =
        if (blocks.hasNext) Set(blocks.next) else Set.empty[BlockId]
      context.reply(result)
    case ReplicateOneBlock(_, blockId, _) =>
      val future = Future {
        Thread.sleep(pauseMillis)
        replicated.add(blockId)
        true
      }
      future.foreach(context.reply)
    case GetMemoryStatus => context.reply(memStatus)
    case GetSizeOfBlocks(bs) => context.reply(bs.mapValues(blocks => blocks.size * sizeOfBlock))
  }
}

// Turns an RpcEndpoint into RpcEndpointRef by calling receive and reply directly
private case class DummyRef(endpoint: RpcEndpoint) extends RpcEndpointRef(new SparkConf()) {
  def address: RpcAddress = null
  def name: String = null
  def send(message: Any): Unit = endpoint.receive(message)
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    val context = new DummyRpcCallContext[T]
    endpoint.receiveAndReply(context)(message)
    context.result
  }
}

// saves values you put in context.reply
private class DummyRpcCallContext[T] extends RpcCallContext {
  val promise: Promise[T] = Promise[T]()
  def result: Future[T] = promise.future
  def reply(response: Any): Unit = promise.success(response.asInstanceOf[T])
  def sendFailure(e: Throwable): Unit = ()
  def senderAddress: RpcAddress = null
}

