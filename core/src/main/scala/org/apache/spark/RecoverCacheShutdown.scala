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
final private class RecoverCacheShutdown(
    state: RecoverCacheShutdownState,
    conf: SparkConf)
  extends Logging {

  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("recover-cache-shutdown-pool")
  private implicit val asyncExecutionContext = ExecutionContext.fromExecutorService(threadPool)

  /**
   * Start the recover cache shutdown process for these executors
   * @param execIds the executors to start shutting down
   */
  def startExecutorKill(execIds: Seq[String]): Unit = {
    logDebug(s"shutdown $execIds")
    checkForReplicableBlocks(execIds)
  }

  /**
   * Stops all thread pools
   * @return
   */
  def stop(): java.util.List[Runnable] = {
    threadPool.shutdownNow()
    state.stop()
  }

  /**
   * Get list of cached blocks from BlockManagerMaster. If there are cached blocks, replicate them,
   * otherwise kill the executors
   * @param execIds the executors to check
   */
  private def checkForReplicableBlocks(execIds: Seq[String]) = state.getBlocks(execIds).foreach {
    case (executorId, NoMoreBlocks) => state.killExecutor(executorId)
    case (executorId, NotEnoughMemory) => state.killExecutor(executorId)
    case (executorId, HasCachedBlocks) => replicateBlocks(executorId)
  }

  /**
   * Replicate one cached block on an executor. If there are more, repeat. If there are none, check
   * with the block manager master again. If there is an error, go ahead and kill executor.
   * @param execId the executor to save a block one
   */
  private def replicateBlocks(execId: String): Unit = state.replicateFirstBlock(execId).onComplete {
    case scala.util.Success(true) => replicateBlocks(execId)
    case scala.util.Success(false) => checkForReplicableBlocks(Seq(execId))
    case Failure(f) =>
      logWarning("Error trying to replicate blocks", f)
      state.killExecutor(execId)
  }
}

private object RecoverCacheShutdown {
  def apply(eam: ExecutorAllocationManager, conf: SparkConf): RecoverCacheShutdown = {
    val bmme = SparkEnv.get.blockManager.master.driverEndpoint
    val state = new RecoverCacheShutdownState(bmme, eam, conf)
    new RecoverCacheShutdown(state, conf)
  }
}

/**
 * Private class that holds state for all the executors being shutdown.
 * @param blockManagerMasterEndpoint blockManagerMasterEndpoint
 * @param executorAllocationManager ExecutorAllocationManager
 * @param conf spark conf
 */
final private class RecoverCacheShutdownState(
   blockManagerMasterEndpoint: RpcEndpointRef,
   executorAllocationManager: ExecutorAllocationManager,
   conf: SparkConf
 ) extends Logging {

  type ExecMap[T] = mutable.Map[String, T]

  private val forceKillAfterS =
    conf.getTimeAsSeconds("spark.dynamicAllocation.recoverCachedData.timeout", "120s")
  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("recover-cache-shutdown-timers")

  private val blocksToSave: ExecMap[mutable.PriorityQueue[RDDBlockId]] = new mutable.HashMap
  private val savedBlocks: ExecMap[mutable.HashSet[RDDBlockId]] = new mutable.HashMap
  private val killTimers: ExecMap[ScheduledFuture[_]] = new mutable.HashMap

  /**
   * Query Block Manager Master for cached blocks.
   * @param execIds the executors to query
   * @return a map of executorId to its replication state.
   */
  def getBlocks(execIds: Seq[String]): Map[String, ExecutorReplicationState] = synchronized {
    logDebug(s"getting all RDD blocks for $execIds")
    execIds.map { id =>
      if (isThereEnoughMemory(id)) {
        updateBlockState(id)
        val blocksRemain = blocksToSave.get(id).exists(_.nonEmpty)
        id -> (if (blocksRemain) HasCachedBlocks else NoMoreBlocks)
      } else {
        id -> NotEnoughMemory
      }
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

  private def blockList = for {
    (id, queue) <- blocksToSave.toSeq
    block <- queue
  } yield (id, block)

  private def updateBlockState(execId: String): Unit = {
    val blocks: mutable.Set[RDDBlockId] = blockManagerMasterEndpoint
      .askSync[collection.Set[BlockId]](GetCachedBlocks(execId))
      .flatMap(_.asRDDId)(collection.breakOut)

    blocks --= savedBlocks.getOrElse(execId, new mutable.HashSet)
    blocksToSave(execId) = mutable.PriorityQueue[RDDBlockId](blocks.toSeq: _*)(Ordering.by(_.rddId))
    killTimers.getOrElseUpdate(execId, scheduleKill(new KillRunner(execId)))
  }

  class KillRunner(execId: String) extends Runnable { def run(): Unit = killExecutor(execId) }
  private def scheduleKill(k: KillRunner) = scheduler.schedule(k, forceKillAfterS, TimeUnit.SECONDS)

  /**
   * Ask block manager master to replicate one cached block.
   * @param execId the executor to replicate a block from
   * @return false if there are no more blocks or if there is an issue
   */
  def replicateFirstBlock(execId: String): Future[Boolean] = synchronized {
    logDebug(s"saveFirstBlock $execId")
    blocksToSave.get(execId) match {
      case Some(p) if p.nonEmpty => doReplication(execId, p.dequeue())
      case _ => Future.successful(false)
    }
  }

  private def doReplication(execId: String, blockId: RDDBlockId): Future[Boolean] = {
    logDebug(s"saving $blockId")
    val savedBlocksForExecutor = savedBlocks.getOrElseUpdate(execId, new mutable.HashSet)
    savedBlocksForExecutor += blockId
    val replicateMessage = ReplicateOneBlock(execId, blockId, blocksToSave.keys.toSeq)
    blockManagerMasterEndpoint.askSync[Future[Boolean]](replicateMessage)
  }

  /**
   * Ask ExecutorAllocationManager to kill executor and clean up state
   * Note executorAllocationManger.killExecutors is blocking, this function should be fixed
   * to use a non blocking version
   * @param execId the executor to kill
   */
  def killExecutor(execId: String): Unit = synchronized {
    logDebug(s"Sending request to kill $execId")
    killTimers.get(execId).foreach(_.cancel(false))
    killTimers -= execId
    blocksToSave -= execId
    savedBlocks -= execId
    executorAllocationManager.killExecutors(Seq(execId))
  }

  def stop(): java.util.List[Runnable] = scheduler.shutdownNow()
}

private sealed trait ExecutorReplicationState
private case object NoMoreBlocks extends ExecutorReplicationState
private case object HasCachedBlocks extends ExecutorReplicationState
private case object NotEnoughMemory extends ExecutorReplicationState
