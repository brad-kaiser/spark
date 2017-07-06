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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.reflect.ClassTag

import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar

import org.apache.spark.rpc._
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages.{GetCachedBlocks, ReplicateOneBlock}

class GracefulShutdownSuite extends SparkFunSuite with MockitoSugar with Matchers {
  test("GracefulShutdown will take blocks until empty and then kill executor") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(2, 1))
    val bmme = FakeBMM(1, blocks.iterator)
    val bmmeRef = DummyRef(bmme)
    val gss = new GracefulShutdownState(bmmeRef, eam, conf)
    val gracefulShutdown = new GracefulShutdown(gss, conf)

    when(eam.killExecutors(Seq("1"))).thenReturn(Seq("1"))

    gracefulShutdown.shutdown(Seq("1"))
    Thread.sleep(1000)
    verify(eam).killExecutors(Seq("1"))
    bmme.replicated.asScala.toSeq shouldBe blocks
  }

  test("GracefulShutdown will kill executor if it takes too long to replicate") {
    val conf = new SparkConf().set("spark.dynamicAllocation.recoverCachedData.timeout", "1s")
    val eam = mock[ExecutorAllocationManager]
    val blocks = Set(RDDBlockId(1, 1), RDDBlockId(2, 1), RDDBlockId(3, 1), RDDBlockId(4, 1))
    val bmme = FakeBMM(600, blocks.iterator)
    val bmmeRef = DummyRef(bmme)
    val gss = new GracefulShutdownState(bmmeRef, eam, conf)
    val gracefulShutdown = new GracefulShutdown(gss, conf)

    gracefulShutdown.shutdown(Seq("1"))
    Thread.sleep(1010)
    verify(eam, times(1)).killExecutors(Seq("1"))
    bmme.replicated.size shouldBe 1
  }

  test("shutdown timer will get cancelled if replication finishes") {
    val conf = new SparkConf().set("spark.dynamicAllocation.recoverCachedData.timeout", "1s")
    val eam = mock[ExecutorAllocationManager]
    val blocks = Set(RDDBlockId(1, 1))
    val bmme = FakeBMM(1, blocks.iterator)
    val bmmeRef = DummyRef(bmme)
    val gss = new GracefulShutdownState(bmmeRef, eam, conf)
    val gracefulShutdown = new GracefulShutdown(gss, conf)

    gracefulShutdown.shutdown(Seq("1"))
    Thread.sleep(1100)
    verify(eam, times(1)).killExecutors(Seq("1")) // should be killed once not twice
  }

  test("Blocks don't get replicated more than once") {
    val conf = new SparkConf()
    val eam = mock[ExecutorAllocationManager]
    val blocks = Seq(RDDBlockId(1, 1), RDDBlockId(1, 1), RDDBlockId(1, 1))
    val bmme = FakeBMM(1, blocks.iterator)
    val bmmeRef = DummyRef(bmme)
    val gss = new GracefulShutdownState(bmmeRef, eam, conf)
    val gracefulShutdown = new GracefulShutdown(gss, conf)

    gracefulShutdown.shutdown(Seq("1"))
    Thread.sleep(100)
    bmme.replicated.size shouldBe 1
    bmme.replicated.asScala.toSeq shouldBe Seq(RDDBlockId(1, 1))
  }


}

private case class FakeBMM(pauseMillis: Int, blocks: Iterator[BlockId])
  extends ThreadSafeRpcEndpoint {
  val rpcEnv = null
  val replicated = new ConcurrentLinkedQueue[BlockId]()

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCachedBlocks(_) =>
      val result: collection.Set[BlockId] =
        if (blocks.hasNext) Set(blocks.next) else Set.empty[BlockId]
      context.reply(result)
    case ReplicateOneBlock(executorId, blockId, _) =>
      val future = Future {
        Thread.sleep(pauseMillis)
        replicated.add(blockId)
        true
      }
      context.reply(future)
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
    Future.successful(context.result)
  }
}

// saves values you put in context.reply
private class DummyRpcCallContext[T] extends RpcCallContext {
  var result: T = _
  def reply(response: Any): Unit = result = response.asInstanceOf[T]
  def sendFailure(e: Throwable): Unit = ()
  def senderAddress: RpcAddress = null
}

