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
import org.apache.spark.internal.config.DYN_ALLOCATION_RECOVER_CACHE_TIMEOUT
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.storage.{BlockId, BlockManagerId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages.{GetCachedBlocks, GetMemoryStatus, GetSizeOfBlocks, ReplicateOneBlock}
import org.apache.spark.util.ThreadUtils

/**
 * Responsible for asynchronously replicating all of an executors cached blocks, and then shutting
 * it down.
 */
final private class CacheRecoveryManager(
    state: CacheRecoveryManagerState,
    conf: SparkConf)
  extends Logging {

  private val threadPool = ThreadUtils.newDaemonCachedThreadPool("recover-cache-shutdown-pool")
  private implicit val asyncExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(threadPool)

  /**
   * Start the recover cache shutdown process for these executors
   *
   * @param execIds the executors to start shutting down
   */
  def startExecutorKill(execIds: Seq[String]): Unit = {
    logDebug(s"Recover cached data before shutting down executors ${execIds.mkString(", ")}.")
    checkForReplicableBlocks(execIds)
  }

  /**
   * Stops all thread pools
   *
   * @return
   */
  def stop(): java.util.List[Runnable] = {
    threadPool.shutdownNow()
    state.stop()
  }

  /**
   * Get list of cached blocks from BlockManagerMaster. If there are cached blocks, replicate them,
   * otherwise kill the executors
   *
   * @param execIds the executors to check
   */
  private def checkForReplicableBlocks(execIds: Seq[String]) = state.getBlocks(execIds).foreach {
    case (executorId, HasCachedBlocks) => replicateBlocks(executorId)
    case (executorId, NoMoreBlocks | NotEnoughMemory) => state.killExecutor(executorId)
  }

  /**
   * Replicate one cached block on an executor. If there are more, repeat. If there are none, check
   * with the block manager master again. If there is an error, go ahead and kill executor.
   *
   * @param execId the executor to save a block one
   */
  private def replicateBlocks(execId: String): Unit = {
    import scala.util.Success
    val (response, blockId) = state.replicateFirstBlock(execId)
    response.onComplete {
      case Success(true) =>
        logTrace(s"Finished replicating block ${blockId.getOrElse("unknown")} on exec $execId.")
        replicateBlocks(execId)
      case Success(false) =>
        checkForReplicableBlocks(Seq(execId))
      case Failure(f) =>
        logWarning(s"Error trying to replicate block ${blockId.getOrElse("unknown")}.", f)
        state.killExecutor(execId)
    }
  }
}

private object CacheRecoveryManager {
  def apply(eam: ExecutorAllocationManager, conf: SparkConf): CacheRecoveryManager = {
    val bmme = SparkEnv.get.blockManager.master.driverEndpoint
    val state = new CacheRecoveryManagerState(bmme, eam, conf)
    new CacheRecoveryManager(state, conf)
  }
}

/**
 * Private class that holds state for all the executors being shutdown.
 *
 * @param blockManagerMasterEndpoint blockManagerMasterEndpoint
 * @param executorAllocationManager ExecutorAllocationManager
 * @param conf spark conf
 */
final private class CacheRecoveryManagerState(
   blockManagerMasterEndpoint: RpcEndpointRef,
   executorAllocationManager: ExecutorAllocationManager,
   conf: SparkConf
 ) extends Logging {

  private val forceKillAfterS = conf.get(DYN_ALLOCATION_RECOVER_CACHE_TIMEOUT)
  private val scheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("recover-cache-shutdown-timers")

    private val blocksToSave = new mutable.HashMap[String, mutable.PriorityQueue[RDDBlockId]]
    private val savedBlocks = new mutable.HashMap[String, mutable.HashSet[RDDBlockId]]
    private val killTimers = new mutable.HashMap[String, ScheduledFuture[_]]

  /**
   * Query Block Manager Master for cached blocks.
   *
   * @param execIds the executors to query
   * @return a map of executorId to its replication state.
   */
  def getBlocks(execIds: Seq[String]): Map[String, ExecutorReplicationState] = synchronized {
    val isThereEnoughMemory = checkFreeMem(execIds)

    isThereEnoughMemory.map {
      case (execId, true) =>
        updateBlockState(execId)
        val blocksRemain = blocksToSave.get(execId).exists(_.nonEmpty)
        execId -> (if (blocksRemain) HasCachedBlocks else NoMoreBlocks)
      case (execId, false) =>
        execId -> NotEnoughMemory
    }
  }

  /**
   * Checks to see if there is enough memory on the cluster to hold the cached blocks on these
   * executors. Checks on a per executor basis from the smallest to the largest.
   *
   * @param execIds the executors to query
   * @return a map of executor ids to whether or not there is enough free memory for them.
   */
  private def checkFreeMem(execIds: Seq[String]): Map[String, Boolean] = {
    val execIdsToShutDown = execIds.toSet
    val allExecMemStatus: Map[String, (Long, Long)] = blockManagerMasterEndpoint
      .askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
      .map { case (blockManagerId, mem) => blockManagerId.executorId -> mem }

    val (expiringMemStatus, remainingMemStatus) = allExecMemStatus.partition {
      case (k, v) => execIdsToShutDown.contains(k)
    }
    val freeMemOnRemaining = remainingMemStatus.values.map(_._2).sum

    val alreadyReplicatedBlocks: Map[String, Set[RDDBlockId]] =
      savedBlocks.filterKeys(execIdsToShutDown).map { case (k, v) => k -> v.toSet }.toMap

    val getSizes = GetSizeOfBlocks(alreadyReplicatedBlocks)
    val alreadyReplicatedBytes = blockManagerMasterEndpoint.askSync[Map[String, Long]](getSizes)

    val bytesToReplicate: Seq[(String, Long)] =
      expiringMemStatus.map { case (execId, (maxMem, remainingMem)) =>
        val alreadyReplicated = alreadyReplicatedBytes.getOrElse(execId, 0L)
        val usedMem = maxMem - remainingMem
        val toBeReplicatedMem = usedMem - alreadyReplicated
        execId -> toBeReplicatedMem
      }.toSeq.sortBy { case (k, v) => v }

    bytesToReplicate.scan(("start", freeMemOnRemaining)) {
      case ((_, remainingMem), (execId, mem)) => (execId, remainingMem - mem)
    }.drop(1)
      .toMap
      .mapValues(freeMem => freeMem >= 0)
  }

  /**
   * Gets a list of cached blocks from the block manager master and updates.
   * CacheRecoveryManagerState.
   *
   * @param execId an executor Id
   */
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
   *
   * @param execId the executor to replicate a block from
   * @return false if there are no more blocks or if there is an issue
   */
  def replicateFirstBlock(execId: String): (Future[Boolean], Option[RDDBlockId]) = synchronized {
    logDebug(s"Replicate block on executor $execId.")
    blocksToSave.get(execId) match {
      case Some(p) if p.nonEmpty => doReplication(execId, p.dequeue())
      case _ => (Future.successful(false), None)
    }
  }

  private def doReplication(
      execId: String,
      blockId: RDDBlockId): (Future[Boolean], Option[RDDBlockId]) = {
    val savedBlocksForExecutor = savedBlocks.getOrElseUpdate(execId, new mutable.HashSet)
    savedBlocksForExecutor += blockId
    val replicateMessage = ReplicateOneBlock(execId, blockId, blocksToSave.keys.toSeq)
    logTrace(s"Started replicating block $blockId on exec $execId.")
    val future = blockManagerMasterEndpoint.askSync[Future[Boolean]](replicateMessage)
    (future, Some(blockId))
  }

  /**
   * Ask ExecutorAllocationManager to kill executor and clean up state
   * Note executorAllocationManger.killExecutors is blocking, this function should be fixed
   * to use a non blocking version
   * @param execId the executor to kill
   */
  def killExecutor(execId: String): Unit = synchronized {
    logDebug(s"Send request to kill executor $execId.")
    killTimers.remove(execId).foreach(_.cancel(false))
    blocksToSave -= execId
    savedBlocks -= execId
    executorAllocationManager.killExecutors(Seq(execId), forceIfPending = true)
  }

  def stop(): java.util.List[Runnable] = scheduler.shutdownNow()
}

private sealed trait ExecutorReplicationState
private case object NoMoreBlocks extends ExecutorReplicationState
private case object HasCachedBlocks extends ExecutorReplicationState
private case object NotEnoughMemory extends ExecutorReplicationState
