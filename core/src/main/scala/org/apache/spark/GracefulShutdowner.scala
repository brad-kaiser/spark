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

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Failure

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.storage.BlockManagerMessages.{GetCachedBlocks, ReplicateOneBlock}

// TODO bk check for tasks?
// TODO bk use better executionContext
final private[spark] case class GracefulShutdowner(endpoint: RpcEndpointRef) extends Logging {
  import scala.concurrent.ExecutionContext.Implicits.global

  def shutdown(executorIds: Seq[String]): Unit = {
    logDebug(s"shutdown $executorIds")
    endpoint.askSync[Map[String, EmptyState]](GetBlocks(executorIds)).foreach {
      case (executorId, Empty) => endpoint.send(KillExecutor(executorId))
      case (executorId, NotEmpty) => saveBlocks(executorId)
    }
  }

  private def saveBlocks(executorId: String): Unit = endpoint
    .askSync[Future[Boolean]](SaveFirstBlock(executorId))
    .onComplete {
      case scala.util.Success(true) => saveBlocks(executorId)
      case scala.util.Success(false) => shutdown(Seq(executorId))
      case Failure(f) =>
        logWarning("Error trying to replicate blocks", f)
        endpoint.send(KillExecutor(executorId))
    }
}

object GracefulShutdowner {
  val endpointName = "graceful-shutdown"

  def apply(rpcEnv: RpcEnv, eam: ExecutorAllocationManager): GracefulShutdowner = {
    val endpoint = rpcEnv.setupEndpoint(endpointName, GracefulShutdownEndpoint(rpcEnv,
      SparkEnv.get.blockManager.master.driverEndpoint, eam))
    GracefulShutdowner(endpoint)
  }
}

private final case class GracefulShutdownEndpoint(
   rpcEnv: RpcEnv,
   blockManagerMasterEndpoint: RpcEndpointRef,
   executorAllocationManager: ExecutorAllocationManager
 ) extends ThreadSafeRpcEndpoint with Logging {

  private val blocksToSave: mutable.Map[String, mutable.PriorityQueue[RDDBlockId]] =
    mutable.HashMap.empty
  private val savedBlocks: mutable.Map[String, mutable.HashSet[RDDBlockId]] =
    mutable.HashMap.empty
      .withDefault(_ => mutable.HashSet.empty[RDDBlockId])

  override def receive: PartialFunction[Any, Unit] = {
    case KillExecutor(executorId) => killExecutor(executorId)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SaveFirstBlock(executorId) => context.reply(saveFirstBlock(executorId))
    case GetBlocks(executorIds) => context.reply(getBlocks(executorIds))
  }

  private def getBlocks(executorIds: Seq[String]): Map[String, EmptyState] = {
    logDebug(s"getting all RDD blocks for $executorIds")
    executorIds.map { executorId =>
      val blocks: mutable.Set[RDDBlockId] = blockManagerMasterEndpoint
        .askSync[collection.Set[BlockId]](GetCachedBlocks(executorId))
        .flatMap(_.asRDDId)(collection.breakOut)

      blocks --= savedBlocks(executorId)

      val queue = mutable.PriorityQueue[RDDBlockId](blocks.toSeq: _*)(Ordering.by(_.rddId))
      blocksToSave += (executorId -> queue)

      (executorId, if (queue.isEmpty) Empty else NotEmpty)
    }(collection.breakOut)
  }

  private def saveFirstBlock(executorId: String): Future[Boolean] = {
    logDebug(s"saveFirstBlock $executorId")
    blocksToSave.get(executorId) match {
      case Some(p) if p.nonEmpty =>
        val blockId = p.dequeue()
        savedBlocks.put(executorId, savedBlocks(executorId) += blockId)
        blockManagerMasterEndpoint.askSync[Future[Boolean]](
          ReplicateOneBlock(executorId, blockId, blocksToSave.keys.toSeq))
      case _ => Future.successful(false)
    }
  }

  private def killExecutor(executorId: String): Unit = {
    logDebug(s"done replicating blocks for $executorId")
    blocksToSave -= executorId
    savedBlocks -= executorId
    executorAllocationManager.killExecutors(Seq(executorId))
  }
}

private sealed trait GracefulShutdownMessage
private final case class GetBlocks(executorIds: Seq[String]) extends GracefulShutdownMessage
private final case class SaveFirstBlock(executorId: String) extends GracefulShutdownMessage
private final case class KillExecutor(executorId: String) extends GracefulShutdownMessage

private trait EmptyState
private case object Empty extends EmptyState
private case object NotEmpty extends EmptyState


