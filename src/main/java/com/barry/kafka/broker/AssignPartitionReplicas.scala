package com.barry.kafka.broker

import kafka.admin.AdminUtils.{rand}

import scala.collection.{Map, Seq, mutable}

/**
 * author borui.shao@earlydata.com
 * Date 2020/8/3 10:01 AM
 * VERSION 1.0
 * Describe TODO
 **/
object AssignPartitionReplicas {

  /**
   * These code excerpt from kafka sources AdminUtils.
   * The goal is to assign replicas for each partition as uniformity as it can.
   * @param nPartitions 分区数
   * @param replicationFactor 副本因子
   * @param brokerList broker 列表
   * @param fixedStartIndex 第一个副本分配的位置 默认 -1
   * @param startPartitionId 第一个分区的编号 默认 -1
   * @return Map(PartitionID,{list of replication Id}
   */
  private def assignReplicasToBrokersRackUnaware(nPartitions: Int,
                                                 replicationFactor: Int,
                                                 brokerList: Seq[Int],
                                                 fixedStartIndex: Int,
                                                 startPartitionId: Int): Map[Int, Seq[Int]] = {
    val ret = mutable.Map[Int, Seq[Int]]()
    val brokerArray = brokerList.toArray
    //起始的副本index，默认是个小于broker数的随机数，以保证多topic时副本分布的均衡，不要每个起始副本都落在同一个broker上
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
    //因为默认startPartitionId is -1, so if you create topic by default, the partitionId will be 0.
    var currentPartitionId = math.max(0, startPartitionId)
    //the interval of each replica
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerArray.length)
    //loop partitions, distribute replica for each partition
    for (_ <- 0 until nPartitions) {
      //if currentPartitionId is the last one interval add 1
      if (currentPartitionId > 0 && (currentPartitionId % brokerArray.length == 0))
        nextReplicaShift += 1
      //init the first index of replica of partition, goal is make first replica move on.
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerArray.length
      val replicaBuffer = mutable.ArrayBuffer(brokerArray(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaBuffer += brokerArray(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerArray.length))
      ret.put(currentPartitionId, replicaBuffer)
      currentPartitionId += 1
    }
    ret
  }

  /**
   * 这个算法能保证副本index向后轮询
   * @param firstReplicaIndex 第一个副本的index
   * @param secondReplicaShift 副本间步长
   * @param replicaIndex 第n+2个副本 ：传入的是0 代表是第二个副本
   * @param nBrokers
   * @return 这个副本的index
   */
  private def replicaIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }


}
