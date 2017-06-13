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

import scala.util.Try

import org.scalatest.{BeforeAndAfterEach, Matchers}

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.network.TransportContext
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.server.TransportServer
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

class RecoverCachedDataSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach {

  var shuffleService: TransportServer = _
  var conf: SparkConf = _
  var sc: SparkContext = _

  private def makeBaseConf() = new SparkConf()
    .setAppName("test")
    .setMaster("local-cluster[4, 1, 512]")
    .set("spark.executor.memory", "512m")
    .set("spark.shuffle.service.enabled", "true")
    .set("spark.shuffle.service.dont.start", "true")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.dynamicAllocation.recoverCachedData", "true")
    .set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "1s")
    .set("spark.dynamicAllocation.executorIdleTimeout", "1s")
    .set("spark.executor.instances", "1")
    .set("spark.dynamicAllocation.initialExecutors", "4")
    .set("spark.dynamicAllocation.minExecutors", "3")

  override def beforeEach(): Unit = {
    conf = makeBaseConf()
    val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numUsableCores = 4)
    val rpcHandler = new ExternalShuffleBlockHandler(transportConf, null)
    val transportContext = new TransportContext(transportConf, rpcHandler)
    shuffleService = transportContext.createServer()
    conf.set("spark.shuffle.service.port", shuffleService.getPort.toString)

  }

  override def afterEach(): Unit = {
    sc.stop()
    conf = null
    shuffleService.close()
  }

  type BInfo = Map[BlockId, Map[BlockManagerId, BlockStatus]]
  private def getLocations(sc: SparkContext, rdd: RDD[_]): BInfo = {
    import scala.collection.breakOut
    val blockIds: Array[BlockId] = rdd.partitions.map(p => RDDBlockId(rdd.id, p.index))
    blockIds.map { id =>
      id -> Try(sc.env.blockManager.master.getBlockStatus(id)).getOrElse(Map.empty)
    }(breakOut)
  }

  // make sure that the unit test working directory is the spark home directory
  // otherwise test will hang indefinitely
  // scalastyle:off println
  test("cached data is replicated before dynamic de-allocation") {
    sc = new SparkContext(conf)
    sc.jobProgressListener.waitUntilExecutorsUp(4, 60000)

    val rdd = sc.parallelize(1 to 1000, 4).map(_ * 4).cache()
    rdd.reduce(_ + _) shouldBe 2002000
    sc.getExecutorIds().size shouldBe 4
    getLocations(sc, rdd).foreach(println)
    getLocations(sc, rdd).forall{ case (id, map) => map.nonEmpty } shouldBe true

    println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX sleeping")
    Thread.sleep(5000)
    println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX done")
    sc.getExecutorIds().size shouldBe 3
    getLocations(sc, rdd).foreach(println)
    getLocations(sc, rdd).forall{ case (id, map) => map.nonEmpty } shouldBe true
  }

  // TODO bk sometimes only 2 executors shut down not 3
  test("dont fail if a bunch of executors are shut down at once") {
    conf.set("spark.dynamicAllocation.minExecutors", "1")
    sc = new SparkContext(conf)
    sc.jobProgressListener.waitUntilExecutorsUp(2, 60000)

    val rdd = sc.parallelize(1 to 1000, 4).map(_ * 4).cache()
    rdd.reduce(_ + _) shouldBe 2002000
    sc.getExecutorIds().size shouldBe 4
    getLocations(sc, rdd).foreach(println)
    getLocations(sc, rdd).forall{ case (id, map) => map.nonEmpty } shouldBe true

    Thread.sleep(2000)
    sc.getExecutorIds().size shouldBe 1
    getLocations(sc, rdd).foreach(println)
    getLocations(sc, rdd).forall{ case (id, map) => map.nonEmpty } shouldBe true
  }

  test("When node memory is limited we are intelligent about replicating data ") {}


}
