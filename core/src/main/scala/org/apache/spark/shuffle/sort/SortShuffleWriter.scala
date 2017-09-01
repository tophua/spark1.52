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

package org.apache.spark.shuffle.sort

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver, ShuffleWriter}
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.ExternalSorter

private[spark] class SortShuffleWriter[K, V, C](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, C],
    mapId: Int,//对应RDD的partionsID
    context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency //shuffle依赖

  private val blockManager = SparkEnv.get.blockManager//块管理器

  private var sorter: SortShuffleFileWriter[K, V] = null

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  //我们正在停止吗？ 因为map任务可以使用success = true调用stop（）
  // 然后在success = false的情况下调用stop（）得到异常，我们想确保我们不尝试删除文件等两次。
  //我们在停止的过程中,因为map任务调用停止成功能为true,
  private var stopping = false

  private var mapStatus: MapStatus = null

  private val writeMetrics = new ShuffleWriteMetrics()
  context.taskMetrics.shuffleWriteMetrics = Some(writeMetrics)

  /** 
   *  Write a bunch of records to this task's output 
   *  把RDD分区中的数据写入文件
   * */
  
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    /** 
    * aggregateByKey默认情况下它的ShuffleDependency 
    * Map-side Join会将数据从不同的dataset中取出,连接起来并放到相应的某个Mapper中处理,
    * 因此key相同的数据肯定会在同一个Mapper里面一起得到处理的
    * */  
    sorter = if (dep.mapSideCombine) {//是否需要在worker端进行combine操作聚合
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      //创建ExternalSorter实例,然后调用insertAll将计算结果写入缓存
      new ExternalSorter[K, V, C](
        dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else if (SortShuffleWriter.shouldBypassMergeSort(
        SparkEnv.get.conf, dep.partitioner.numPartitions, aggregator = None, keyOrdering = None)) {
	    //传递到Reduce端再做合并(merge)操作的阈值
	    //如果numPartitions小于bypassMergeThreshold,则不需要在Executor执行聚合和排序操作
	    //只需要将各个Partition直接写到Executor的存储文件,最后在reduce端再做串联
	    //通过配置spark.shuffle.sort.bypassMergeThreshold可以修改bypassMergeThreshold的大小

      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need local aggregation and sorting, write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleWriter[K, V](SparkEnv.get.conf, blockManager, dep.partitioner,
        writeMetrics, Serializer.getSerializer(dep.serializer))
    } else {
      //在这种情况下,分类不聚合或也不排序,因为我们不在乎键是否在每个分区中进行排序,如果正在运行的操作sortbykey,这将在减少方面
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      //在这种情况下,我们既不将聚合器也不传递给分拣机,因为我们不关心每个分区中的键是否被排序,如果正在运行的操作是sortByKey,那么这将在reduce方面完成
      new ExternalSorter[K, V, V](
        aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    //将计算结果写入缓存
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    //返回输出的文件对象,文件名根据shuffleid、mapId、reduceId命名,文件路径在BlockManager的DiskStore存储文件的位置  
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    //获取或创建临时数据文件
    val tmp = Utils.tempFileWith(output)    
    //blockId和outputFile的文件名生成算法一样,传入的参数也一样,所以blockId和outputFile的文件名相同
    //mapId对应RDD的partionsID
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
    //将中间结果持久化,返回中间结果存储的长度
    val partitionLengths = sorter.writePartitionedFile(blockId, context, tmp)
    //将Shuffle map后Stage2每个partition在outputFile的起始地址记录到index索引文件中
    //mapId对应RDD的partionsID
    shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
    //mapStatus是ShuffleMapTask的返回值 
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  }

  /** 
   *  Close this writer, passing along whether the map completed 
   *  关闭写操作,是否完成Map任务的传递
   *  */
  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        return Option(mapStatus)
      } else {
        // The map task failed, so delete our output data.
        //map任务失败,删除输出文件
        //mapId对应RDD的partionsID
        shuffleBlockResolver.removeDataByMap(dep.shuffleId, mapId)
        return None
      }
    } finally {
      //清理存储,包括生成中间文件
      // Clean up our sorter, which may have its own intermediate files
      if (sorter != null) {
        val startTime = System.nanoTime()
        sorter.stop()
        context.taskMetrics.shuffleWriteMetrics.foreach(
          _.incShuffleWriteTime(System.nanoTime - startTime))
        sorter = null
      }
    }
  }
}

private[spark] object SortShuffleWriter {
//标记是否传递到reduce端再做合并排序,即是否直接将各个Partition直接写到Executor的存储文件
  def shouldBypassMergeSort(
      conf: SparkConf,
      numPartitions: Int,
      aggregator: Option[Aggregator[_, _, _]],
      keyOrdering: Option[Ordering[_]]): Boolean = {
    //如果partition数目少于bypassMergeThreshold的值,不需要在Executor执行聚合和排序操作,直接将每个partition写入单独的文件,最后在reduce端再做串联
    val bypassMergeThreshold: Int = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    //如果numPartitions小于bypassMergeThreshold,并且没有聚合和排序函数,则不需要在Executor执行聚合和排序操作
    //只需要将各个Partition直接写到Executor的存储文件,最后在reduce端再做串联
    numPartitions <= bypassMergeThreshold && aggregator.isEmpty && keyOrdering.isEmpty
  }
}
