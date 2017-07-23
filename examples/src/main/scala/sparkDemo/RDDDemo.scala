package sparkDemo

import org.apache.hadoop.hive.ql.exec.persistence.HybridHashTableContainer.HashPartition
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark._
import sparkDemo.RDDcoalesceAndRepartition.sc

/**
  * Created by liush on 17-7-23.
  */
object RDDDemo extends App{

  val sparkConf = new SparkConf().setAppName("SparkHdfsLR").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  /**
    * RDD partition 分区,一个RDD会有一个或多个分区
    */
  val rdd=sc.parallelize( 1 to 100,2)
  //获得分区数
  rdd.partitions.size
  /**
    * RDD preferredLoaction(p),对于分区p而言,返回数据本地化技算的节点
    */
  var data = sc.textFile("hdfs://name-node1:8020/user/hive/warehouse/src/kv1_copy_1.txt")
  val hadoopRDD=data.dependencies(0).rdd
  //获得分默认区数大小
 val size= hadoopRDD.partitions.size
  println("size:"+size)
    //返回RDD每个partition分区数据所存储的位置,如果每一块数据是多份存储,那么就会还回多个机器地址
  //返回每一个数据块所在的机器名或者IP地址,如果每一块数据是多份存储,那么就会还回多个机器地址
  val prelcation=hadoopRDD.preferredLocations(hadoopRDD.partitions(0))
  //WrappedArray(store-node2, store-node3, store-node6)
  println(prelcation)


  /*
  * RDD依赖关系(dependencies)
  **/
  val rddDepend=sc.makeRDD(1 to 10)

  val mapRDD=rddDepend.map(x=>(x,x))
  println(mapRDD.dependencies)
  //该函数根据partitioner函数生成新的ShuffleRDD，将原RDD重新分区
  val shuffleRDD=mapRDD.partitionBy(new HashPartitioner(3))
  println(shuffleRDD.dependencies)

  /**
    * RDD分区计算(compute)
    * RDD的计算都是已partion分区为单位,对于分区p而言,进行迭代计算
    * compute函数只返回相应分区数据的迭代器
    */
  val rddCompute=sc.parallelize( 1 to 10,2)
  val map_RDD=rddCompute.map(a=>a+1)
  val filter_RDD=map_RDD.filter( a=>(a>3))
 /* val context= TaskContext.get() //由于不能获得TaskContext,后续解决
  println("===="+context)
  val itero=filter_RDD.compute(filter_RDD.partitions(0),context)

  println("RDD compute"+itero.toList)
  val itero1=filter_RDD.compute(filter_RDD.partitions(1),context)
  println("RDD compute"+ itero.toList)*/


  /**
    * RDD分区函数(partitioner)每个分区进行数据分割的依据
    */
  val rddPartitions=sc.parallelize( 1 to 10,2).map(x=>(x,x))
  println("partitioner:"+rddPartitions.partitioner)
  val group_rdd=rddPartitions.groupByKey(new org.apache.spark.HashPartitioner(3))
  println("partitioner2:"+group_rdd.partitioner)
  group_rdd.foreach(println _)
 // group_rdd.collect()
  //glom函数将每个分区形成一个数组,内部实现是返回的GlommedRDD
  /*List(
            List((6,CompactBuffer(6)), (3,CompactBuffer(3)), (9,CompactBuffer(9))),
            List((4,CompactBuffer(4)), (1,CompactBuffer(1)), (7,CompactBuffer(7)), (10,CompactBuffer(10))),
            List((8,CompactBuffer(8)), (5,CompactBuffer(5)), (2,CompactBuffer(2))))
   */
  println(group_rdd.glom().map(_.toList).collect().toList)

  //group_rdd.foreachPartition()

}
