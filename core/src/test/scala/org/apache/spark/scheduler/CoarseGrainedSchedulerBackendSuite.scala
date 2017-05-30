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

package org.apache.spark.scheduler

import org.scalatest.Matchers

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{ExternalShuffleBlockHandler, ExternalShuffleClient}
import org.apache.spark.util.{RpcUtils, SerializableBuffer, Utils}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite
  with LocalSparkContext with Matchers {

  test("serialized task larger than max RPC message size") {
    val conf = new SparkConf
    conf.set("spark.rpc.message.maxSize", "1")
    conf.set("spark.default.parallelism", "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

  // make sure that the unit test working directory is the spark home directory
  // otherwise test will hang indefinitely
  // scalastyle:off println
  test("test that cached data is replicated before dynamic de-allocation") {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local-cluster[4, 1, 512]")
    conf.set("spark.executor.memory", "512m")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.dynamicAllocation.testing", "false")
    conf.set("spark.shuffle.service.enabled", "true")
    conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "1s")
    conf.set("spark.dynamicAllocation.executorIdleTimeout", "1s")
    conf.set("spark.dynamicAllocation.initialExecutors", "4")
//    conf.set("spark.dynamicAllocation.recoverCachedData", "true")
    conf.set("spark.dynamicAllocation.minExecutors", "1")

    val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 4)
    val rpcHandler = new ExternalShuffleBlockHandler(transportConf, null)
    val transportContext = new TransportContext(transportConf, rpcHandler)
    val server = transportContext.createServer()
    conf.set("spark.shuffle.service.port", server.getPort.toString)

    sc = new SparkContext(conf)
    sc.env.blockManager.externalShuffleServiceEnabled should equal(true)
    sc.env.blockManager.shuffleClient.getClass should equal(classOf[ExternalShuffleClient])
    sc.schedulerBackend

    println("waiting on executrs up")
    sc.jobProgressListener.waitUntilExecutorsUp(2, 60000)
    println("executors up")

    val rdd = sc.parallelize(1 to 1000, 4)
    println(Utils.isDynamicAllocationEnabled(conf))
    println(rdd.partitions.size)
    println(rdd.count)
    assert(rdd.count === 1000)
    println(sc.getExecutorMemoryStatus)
    println("sleeping  XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    Thread.sleep(9000)
    println(sc.getExecutorMemoryStatus)


    sc.stop()
  }
  // scalastyle:on println

  test("executors can access replicated data after dynamic deallocation") {}
  test("blocks spilled to disk are properly cleaned up after dynamic deallocation.")  {}
  test("Nodes that are actively deallocation won't get new tasks.")  {}
  test("When node memory is limited we are intelligent about replicating data ") {}






}
