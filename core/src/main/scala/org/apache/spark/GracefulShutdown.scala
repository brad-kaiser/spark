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

import java.util.concurrent.{Executors, ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages.{GetCachedBlocks, ReplicateOneBlock}
import org.apache.spark.util.ThreadUtils

/**
 * Responsible for asynchronously replicating all of an executors cached blocks, and then shutting
 * it down.
 */
final private[spark] case class GracefulShutdown(
    endpoint: RpcEndpointRef,
    conf: SparkConf)
  extends Logging {

  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("graceful-shutdown-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  /**
   * Start the graceful shutdown process for these executors
   * @param executorIds
   */
  def shutdown(executorIds: Seq[String]): Unit = {
    logDebug(s"shutdown $executorIds")
    checkBlocks(executorIds)
  }

  /**
   * Get list of cached blocks from BlockManagerMaster. If some remain, save them, otherwise kill
   * the executors
   * @param executorIds
   */
  private def checkBlocks(executorIds: Seq[String]): Unit =
    endpoint.askSync[Map[String, Emptiness]](GetBlocks(executorIds)).foreach {
      case (executorId, Empty) => endpoint.send(KillExecutor(executorId))
      case (executorId, NotEmpty) => saveBlocks(executorId)
    }

  /**
   * Replicate one cached block on an executor. If there are more, repeat. If there are none, check
   * with the block manager master again. If there is an error, go ahead and kill executor.
   * @param executorId
   */
  private def saveBlocks(executorId: String): Unit =
    endpoint.askSync[Future[Boolean]](SaveFirstBlock(executorId))
    .onComplete {
      case scala.util.Success(true) => saveBlocks(executorId)
      case scala.util.Success(false) => checkBlocks(Seq(executorId))
      case Failure(f) =>
        logWarning("Error trying to replicate blocks", f)
        endpoint.send(KillExecutor(executorId))
    }
}

object GracefulShutdown {
  val endpointName = "graceful-shutdown"

  def apply(rpcEnv: RpcEnv, eam: ExecutorAllocationManager, conf: SparkConf): GracefulShutdown = {
    val endpoint = rpcEnv.setupEndpoint(endpointName, new GracefulShutdownEndpoint(rpcEnv,
      SparkEnv.get.blockManager.master.driverEndpoint, eam, conf))
    GracefulShutdown(endpoint, conf)
  }
}

// Thread safe endpoint to handle executor state during graceful shutdown
private class GracefulShutdownEndpoint(
   val rpcEnv: RpcEnv,
   blockManagerMasterEndpoint: RpcEndpointRef,
   executorAllocationManager: ExecutorAllocationManager,
   conf: SparkConf
 ) extends ThreadSafeRpcEndpoint with Logging {

  type ExecMap[T] = mutable.Map[String, T]

  private val forceKillAfterS =
    conf.getTimeAsSeconds("spark.dynamicAllocation.recoverCachedData.timeout", "120s")
  private val killScheduler = Executors.newSingleThreadScheduledExecutor
  private val blocksToSave: ExecMap[mutable.PriorityQueue[RDDBlockId]] = new mutable.HashMap
  private val savedBlocks: ExecMap[mutable.HashSet[RDDBlockId]] = new mutable.HashMap
  private val killTimers: ExecMap[ScheduledFuture[_]] = new mutable.HashMap

  override def receive: PartialFunction[Any, Unit] = {
    case KillExecutor(executorId) => killExecutor(executorId)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SaveFirstBlock(executorId) => context.reply(saveFirstBlock(executorId))
    case GetBlocks(executorIds) => context.reply(getBlocks(executorIds))
  }

  // get blocks from block manager master
  private def getBlocks(executorIds: Seq[String]): Map[String, Emptiness] = {
    logDebug(s"getting all RDD blocks for $executorIds")
    executorIds.map { executorId =>
      val blocks: mutable.Set[RDDBlockId] = blockManagerMasterEndpoint
        .askSync[collection.Set[BlockId]](GetCachedBlocks(executorId))
        .flatMap(_.asRDDId)(collection.breakOut)

      blocks --= savedBlocks.getOrElse(executorId, new mutable.HashSet)

      val queue = mutable.PriorityQueue[RDDBlockId](blocks.toSeq: _*)(Ordering.by(_.rddId))
      blocksToSave(executorId) = queue

      if (!killTimers.contains(executorId)) {
        val killRunnable = new Runnable { def run(): Unit = self.send(KillExecutor(executorId)) }
        val killTimer = killScheduler.schedule(killRunnable, forceKillAfterS, TimeUnit.SECONDS)
        killTimers(executorId) = killTimer
      }

      (executorId, if (queue.isEmpty) Empty else NotEmpty)
    }(collection.breakOut)
  }

  // Ask block manager master to replicate a cached block
  private def saveFirstBlock(executorId: String): Future[Boolean] = {
    logDebug(s"saveFirstBlock $executorId")
    blocksToSave.get(executorId) match {
      case Some(p) if p.nonEmpty =>
        val blockId = p.dequeue()
        val savedBlocksForExecutor = savedBlocks.getOrElseUpdate(executorId, new mutable.HashSet)
        savedBlocksForExecutor += blockId
        blockManagerMasterEndpoint.askSync[Future[Boolean]](
          ReplicateOneBlock(executorId, blockId, blocksToSave.keys.toSeq))
      case _ => Future.successful(false)
    }
  }

  // Ask ExecutorAllocationManager to kill executor and clean up state
  // executorAllocationManager.killExecutors blocks. This might be a problem
  // TODO bk make a non blocking version of kill executors
  private def killExecutor(executorId: String): Unit = {
    logDebug(s"Sending request to kill $executorId")
    killTimers.get(executorId).foreach(_.cancel(false))
    killTimers -= executorId
    blocksToSave -= executorId
    savedBlocks -= executorId

    executorAllocationManager.killExecutors(Seq(executorId))
  }
}

private sealed trait GracefulShutdownMessage
private final case class GetBlocks(executorIds: Seq[String]) extends GracefulShutdownMessage
private final case class SaveFirstBlock(executorId: String) extends GracefulShutdownMessage
private final case class KillExecutor(executorId: String) extends GracefulShutdownMessage

private sealed trait Emptiness
private case object Empty extends Emptiness
private case object NotEmpty extends Emptiness
