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

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.BlockManagerMessages.ReplicateFirstBlock

case class GracefulShutdownEndpoint(
    rpcEnv: RpcEnv,
    blockManagerMaster: RpcEndpointRef,
    executorAllocationManager: ExecutorAllocationManager)
  extends ThreadSafeRpcEndpoint with Logging {

  private val executorsBeingRemoved: mutable.Set[String] = mutable.HashSet.empty[String]

  override def receive: PartialFunction[Any, Unit] = {
    case RemoveExecutors(executorIds) =>
      logDebug("starting to remove Executor")
      executorsBeingRemoved ++= executorIds
      executorIds.foreach { id =>
        blockManagerMaster.ask(ReplicateFirstBlock(id, executorsBeingRemoved.toSeq))
      }
    case ExecutorHasMoreBlocks(executorId) =>
      logDebug("replicated one block, executor has more")
      blockManagerMaster.send(ReplicateFirstBlock(executorId, executorsBeingRemoved.toSeq))
    case ExecutorIsDone(executorId) =>
      logDebug(s"done replicating blocks for $executorId")
      executorsBeingRemoved -= executorId
      executorAllocationManager.killExecutor(Seq(executorId))
  }
}

sealed trait GracefulShutdownMessages
case class RemoveExecutors(executorIds: Seq[String]) extends GracefulShutdownMessages
case class ExecutorHasMoreBlocks(executorId: String) extends GracefulShutdownMessages
case class ExecutorIsDone(executorId: String) extends GracefulShutdownMessages

