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

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite}
import org.apache.spark.util.{RpcUtils, SerializableBuffer, Utils}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext {

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

  test("test replicating cached data after dynamic deallocation") {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local-cluster[4, 1, 1024]")
//    conf.set("spark.shuffle.service.enabled", "true") //TODO bk shuffle service makes test hang for some reason
    conf.set("spark.dynamicAllocation.enabled", "false")
//    conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "1s")
//    conf.set("spark.dynamicAllocation.recoverCachedData", "true")
//    conf.set("spark.dynamicAllocation.minExecutors", "2")

    sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 1000, 4)
    println(Utils.isDynamicAllocationEnabled(conf))
    println(rdd.partitions.size)
    println(rdd.cache)
    println(rdd.count)
    assert(rdd.count === 1000)
    Thread.sleep(500)

    println(sc.persistentRdds)
    println(rdd.partitions.size)


    sc.stop()
  }

}
