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

import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.{BlockId, BlockManagerId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages.{GetCachedBlocks, GetMemoryStatus, GetSizeOfBlocks, ReplicateOneBlock}
import org.apache.spark.util.ThreadUtils

/**
 * Responsible for asynchronously replicating all of an executors cached blocks, and then shutting
 * it down.
 */
final private class GracefulShutdown(
    gss: GracefulShutdownState,
    conf: SparkConf)
  extends Logging {

  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("graceful-shutdown-thread-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  /**
   * Start the graceful shutdown process for these executors
   * @param execIds the executors to start shutting down
   */
  def startExecutorKill(execIds: Seq[String]): Unit = {
    logDebug(s"shutdown $execIds")
    checkForReplicableBlocks(execIds)
  }

  def stop(): java.util.List[Runnable] = {
    threadPool.shutdownNow()
    gss.stop()
  }

  /**
   * Get list of cached blocks from BlockManagerMaster. If some remain, save them, otherwise kill
   * the executors
   * @param execIds the executors to check
   */
  private def checkForReplicableBlocks(execIds: Seq[String]) = gss.getBlocks(execIds).foreach {
    case (executorId, NoMoreBlocks) => gss.killExecutor(executorId)
    case (executorId, NotEnoughMemory) => gss.killExecutor(executorId)
    case (executorId, HasCachedBlocks) => replicateBlocks(executorId)
  }

  /**
   * Replicate one cached block on an executor. If there are more, repeat. If there are none, check
   * with the block manager master again. If there is an error, go ahead and kill executor.
   * @param execId the executor to save a block one
   */
  private def replicateBlocks(execId: String): Unit = gss.replicateFirstBlock(execId).onComplete {
    case scala.util.Success(true) => replicateBlocks(execId)
    case scala.util.Success(false) => checkForReplicableBlocks(Seq(execId))
    case Failure(f) =>
      logWarning("Error trying to replicate blocks", f)
      gss.killExecutor(execId)
  }
}

private object GracefulShutdown {
  def apply(eam: ExecutorAllocationManager, conf: SparkConf): GracefulShutdown = {
    val bmme = SparkEnv.get.blockManager.master.driverEndpoint
    val gss = new GracefulShutdownState(bmme, eam, conf)
    new GracefulShutdown(gss, conf)
  }
}

// Thread safe endpoint to handle executor state during graceful shutdown
private class GracefulShutdownState(
   blockManagerMasterEndpoint: RpcEndpointRef,
   executorAllocationManager: ExecutorAllocationManager,
   conf: SparkConf
 ) extends Logging {

  type ExecMap[T] = mutable.Map[String, T]

  private val forceKillAfterS =
    conf.getTimeAsSeconds("spark.dynamicAllocation.recoverCachedData.timeout", "120s")
  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("graceful-shutdown-timers")

  private val blocksToSave: ExecMap[mutable.PriorityQueue[RDDBlockId]] = new mutable.HashMap
  private val savedBlocks: ExecMap[mutable.HashSet[RDDBlockId]] = new mutable.HashMap
  private val killTimers: ExecMap[ScheduledFuture[_]] = new mutable.HashMap

  // get blocks from block manager master
  def getBlocks(execIds: Seq[String]): Map[String, ExecutorReplicationState] = synchronized {
    logDebug(s"getting all RDD blocks for $execIds")
    execIds.map { id => if (isThereEnoughMemory(id)) updateBlockInfo(id) else id -> NotEnoughMemory
    }(collection.breakOut)
  }

  /**
   *  Is there enough memory on the cluster to replicate the cached data on execId before we delete
   *  it. Take into account all the blocks that are currently on track to be deleted.
   * @param execId the id of the executor we want to delete.
   * @return true if there is room
   */
  private def isThereEnoughMemory(execId: String): Boolean = {
    val currentMemStatus = blockManagerMasterEndpoint
      .askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
      .map { case (blockManagerId, mem) => blockManagerId.executorId -> mem }
    val remaining = currentMemStatus - execId -- blocksToSave.keys

    val thisExecBytes = if (!blocksToSave.contains(execId) && currentMemStatus.contains(execId)) {
      currentMemStatus(execId)._1 - currentMemStatus(execId)._2
    } else {
      0
    }

    remaining.nonEmpty && everyExecutorHasEnoughMemory(remaining, thisExecBytes)
  }

  private def everyExecutorHasEnoughMemory(execMem: Map[String, (Long, Long)], bytes: Long) = {
    val blocksInQueueBytes = blockManagerMasterEndpoint.askSync[Long](GetSizeOfBlocks(blockList))
    val bytesPerExec = (blocksInQueueBytes + bytes) / execMem.size.toFloat
    execMem.forall { case (_, (_, remaining)) => remaining - bytesPerExec >= 0 }
  }

  private def blockList = for ((id, pq) <- blocksToSave.toSeq;  block <- pq) yield (id, block)

  private def updateBlockInfo(execId: String): (String, ExecutorReplicationState) = {
    val blocks: mutable.Set[RDDBlockId] = blockManagerMasterEndpoint
      .askSync[collection.Set[BlockId]](GetCachedBlocks(execId))
      .flatMap(_.asRDDId)(collection.breakOut)
    blocks --= savedBlocks.getOrElse(execId, new mutable.HashSet)
    val queue = mutable.PriorityQueue[RDDBlockId](blocks.toSeq: _*)(Ordering.by(_.rddId))
    blocksToSave(execId) = mutable.PriorityQueue[RDDBlockId](blocks.toSeq: _*)(Ordering.by(_.rddId))
    killTimers.getOrElseUpdate(execId, scheduleKill(new KillRunner(execId)))
    if (queue.isEmpty) execId -> NoMoreBlocks else execId -> HasCachedBlocks
  }

  class KillRunner(execId: String) extends Runnable { def run(): Unit = killExecutor(execId) }
  private def scheduleKill(k: KillRunner) = scheduler.schedule(k, forceKillAfterS, TimeUnit.SECONDS)

  // Ask block manager master to replicate a cached block
  def replicateFirstBlock(execId: String): Future[Boolean] = synchronized {
    logDebug(s"saveFirstBlock $execId")
    blocksToSave.get(execId) match {
      case Some(p) if p.nonEmpty => doReplication(execId, p.dequeue())
      case _ => Future.successful(false)
    }
  }

  private def doReplication(execId: String, blockId: RDDBlockId) = {
    logDebug(s"saving $blockId")
    val savedBlocksForExecutor = savedBlocks.getOrElseUpdate(execId, new mutable.HashSet)
    savedBlocksForExecutor += blockId
    val replicateMessage = ReplicateOneBlock(execId, blockId, blocksToSave.keys.toSeq)
    blockManagerMasterEndpoint.askSync[Future[Boolean]](replicateMessage)
  }

  // Ask ExecutorAllocationManager to kill executor and clean up state
  // executorAllocationManager.killExecutors blocks. This might be a problem
  // TODO bk make a non blocking version of kill executors
  def killExecutor(executorId: String): Unit = synchronized {
    logDebug(s"Sending request to kill $executorId")
    killTimers.get(executorId).foreach(_.cancel(false))
    killTimers -= executorId
    blocksToSave -= executorId
    savedBlocks -= executorId
    executorAllocationManager.killExecutors(Seq(executorId))
  }

  def stop(): java.util.List[Runnable] = scheduler.shutdownNow()
}

private sealed trait ExecutorReplicationState
private case object NoMoreBlocks extends ExecutorReplicationState
private case object HasCachedBlocks extends ExecutorReplicationState
private case object NotEnoughMemory extends ExecutorReplicationState
