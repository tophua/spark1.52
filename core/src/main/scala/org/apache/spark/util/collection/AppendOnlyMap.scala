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

package org.apache.spark.util.collection

import java.util.{ Arrays, Comparator }

import com.google.common.hash.Hashing

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A simple open hash table optimized for the append-only use case, where keys
 * are never removed, but the value for each key may be changed.
 * 当需要对Value进行聚合时,会使用AppendOnlyMap作为buffer,它是一个只支持追加的map,
 * 可以修改某个key对应的value,但不能删除已存在的key.使用它是因为在shuffle的map端,删除key不是必须的
 * This implementation uses quadratic probing with a power-of-2 hash table
 * size, which is guaranteed to explore all spaces for each key (see
 * http://en.wikipedia.org/wiki/Quadratic_probing).
 * AppendOnlyMap也是一个hash map,但它不是像HashMap一样在Hash冲突时采用链接法,而是采用二次探测法
 * 它就不需要采用entry这种对kv对的包装,而是把kv对写同一个object数组里,减少了entry的对象头带来的内存开销
 * 但是二次探测法有个缺点,就是删除元素时比较复杂.
 * The map can support up to `375809638 (0.7 * 2 ^ 29)` elements.
 * 该Mpa支持高达375809638(0.7 * 2 * 29)的元素。
 *
 * 缓存集合算法
  *
 * 处理后的数据,内存使用的是AppendOnlyMap
  * 如果spark.shuffle.spill = false就只用内存,内存使用的是AppendOnlyMap
 * TODO: Cache the hash values of each key? java.util.HashMap does that.
 */
@DeveloperApi
class AppendOnlyMap[K, V](initialCapacity: Int = 64)
    extends Iterable[(K, V)] with Serializable {

  import AppendOnlyMap._

  require(initialCapacity <= MAXIMUM_CAPACITY,//536870912
    s"Can't make capacity bigger than ${MAXIMUM_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")

  private val LOAD_FACTOR = 0.7 //负载因子,常量值等于0.7

  private var capacity = nextPowerOf2(initialCapacity) //容量,初始值等于64
  private var mask = capacity - 1 //计算数据存放位置的掩码值
  private var curSize = 0 //记当当前已经放入data的key与聚合的数量
  private var growThreshold = (LOAD_FACTOR * capacity).toInt //data数组容量增加的阈值

  // Holds keys and values in the same array for memory locality; specifically, the order of
  //保存在同一个内存位置数组中的键和值,具体而言,元素的顺序是
  // elements is key0, value0, key1, value1, key2, value2, etc.
  //数组,初始化大小为2*capacity,data数组的实际大小之所以是capacity的2倍是因为Key和聚合值各占一位
  private var data = new Array[AnyRef](2 * capacity)

  // Treat the null key differently so we can use nulls in "data" to represent empty items.
  //处理空键不同,以便我们可以使用“数据”中的空值来表示空项目。数据是否有null值
  private var haveNullValue = false
  private var nullValue: V = null.asInstanceOf[V]

  // Triggered by destructiveSortedIterator; the underlying data array may no longer be used
  // 引发destructiveSortedIterator,底层的数据数组可能不再使用
  private var destroyed = false
  private val destructionMessage = "Map state is invalid from destructive sorting!"

  /** 
   *  Get the value for a given key 
   *  获取给定键的值
   *  */
  def apply(key: K): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      return nullValue
    }
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        return data(2 * pos + 1).asInstanceOf[V]
      } else if (curKey.eq(null)) {
        return null.asInstanceOf[V]
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V]
  }

  /** 
   *  Set the value for a key 
   *  设置一个键的值
   *  */
  def update(key: K, value: V): Unit = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = value
      haveNullValue = true
      return
    }
    var pos = rehash(key.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (curKey.eq(null)) {
        data(2 * pos) = k
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        //因为我们添加了一个新的key
        incrementSize() // Since we added a new key
        return
      } else if (k.eq(curKey) || k.equals(curKey)) {
        data(2 * pos + 1) = value.asInstanceOf[AnyRef]
        return
      } else {
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
  }

  /**
   * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
   * for key, if any, or null otherwise. Returns the newly updated value.
   * 将key的值设置为updateFunc(hadValue，oldValue),其中oldValue将为key的旧值（如果有）,否则为空, 返回新更新的值。
    * 返回新更新的值,kv是record的每条记录
    */
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
    val k = key.asInstanceOf[AnyRef] //要放入data的Key
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize() //容量增长
      }
      //回调updateFunc函数进行聚合操作
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
    //对于Key进行rehash,计算出这个key在SizeTrackingAppendOnlyMap这个数据结构中的位置
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {     
      //2*pos表示key,2*pos+1表示key对应的value
      val curKey = data(2 * pos) //data(2 * pos)位置的当前Key
      //当前key已经存在于Map中,则需要做combine操作
      if (k.eq(curKey) || k.equals(curKey)) {
        //对Map中缓存的Key的Value进行_ + _操作,updateFunc即是在ExternalSorter.insertAll方法中创建的update函数
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V]) //key的聚合值
        //将新值回写到data(2*pos+1)处,不管data（2*pos + 1)处是否有值
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {//如果当前Map中,data(2*pos)处是null对象
        //调用_ + _操作获取kv的value值  
        val newValue = updateFunc(false, null.asInstanceOf[V]) //key的聚合值
        data(2 * pos) = k //Key
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]//Value
        //因为是Map中新增的K/V,做容量扩容检查
        incrementSize()//递增
        return newValue
      } else {
        //如果当前Map中,data(2*pos)处是空
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }

  /** Iterator method from Iterable
    * 迭代器方法从Iterable*/
  override def iterator: Iterator[(K, V)] = {
    assert(!destroyed, destructionMessage)
    new Iterator[(K, V)] {
      var pos = -1

      /** 
       *  Get the next value we should return from next(), or null if we're finished iterating 
       *  获取下一个值,如果为空完成迭代
       *  */
      def nextValue(): (K, V) = {
        //处理位置- 1视为空值
        if (pos == -1) { // Treat position -1 as looking at the null value          
          if (haveNullValue) {
            return (null.asInstanceOf[K], nullValue)
          }
          pos += 1
        }
        while (pos < capacity) {
          if (!data(2 * pos).eq(null)) {
            return (data(2 * pos).asInstanceOf[K], data(2 * pos + 1).asInstanceOf[V])
          }
          pos += 1
        }
        null
      }

      override def hasNext: Boolean = nextValue() != null

      override def next(): (K, V) = {
        val value = nextValue()
        if (value == null) {
          throw new NoSuchElementException("End of iterator")
        }
        pos += 1
        value
      }
    }
  }

  override def size: Int = curSize

  /** 
   *  Increase table size by 1, rehashing if necessary 
   *  记录数组元素计算自增1
   *  */
  private def incrementSize() {
    curSize += 1 //记录数组元素计算自增1
    //当curSize大于growThreshold,调用growTable方法将capacity容量扩大一倍,即capacity=capacity*2
    if (curSize > growThreshold) {
      growTable()
    }
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
   * 重新散列一个值
   */
  private def rehash(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  /** Double the table's size and re-hash everything */
  //将capacity容量扩大2倍,先创建newCapacity的两倍大小的新数组,将第数组中的元素复制到新数组中,新数组索引位置采用新
  //的rehash(k.hashCode)&mask计算
  protected def growTable() {
    // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow   不会溢出
    val newCapacity = capacity * 2//64X2=128
    require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
    val newData = new Array[AnyRef](2 * newCapacity)//256
    val newMask = newCapacity - 1//127
    // Insert all our old values into the new array. Note that because our old keys are
    // unique, there's no need to check for equality here when we insert.
    //将我们所有的旧值插入到新的数组中,注意因为旧Key值唯一值,当插入时没有必要检查元素相等
    var oldPos = 0
    while (oldPos < capacity) {
      if (!data(2 * oldPos).eq(null)) {
        val key = data(2 * oldPos)
        val value = data(2 * oldPos + 1)
        var newPos = rehash(key.hashCode) & newMask
        var i = 1
        var keepGoing = true
        while (keepGoing) {
          val curKey = newData(2 * newPos)
          if (curKey.eq(null)) {
            newData(2 * newPos) = key //新数组赋值
            newData(2 * newPos + 1) = value//新数组赋值
            keepGoing = false
          } else {
            val delta = i
            newPos = (newPos + delta) & newMask
            i += 1
          }
        }
      }
      oldPos += 1
    }    
    data = newData
    capacity = newCapacity
    mask = newMask
    //growThreshold=0.7X128=89.6
    growThreshold = (LOAD_FACTOR * newCapacity).toInt
  }

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)//highestOneBit方法获取最高位的1位(最左边)的位置
    if (highBit == n) n else highBit << 1 //例如:64左移1等于128
  }

  /**
   * Return an iterator of the map in sorted order. This provides a way to sort the map without
   * using additional memory, at the expense of destroying the validity of the map.    
   * 2)利用Sort,KVArraySortDataFormat以及指定的比较器进行排序,这其中用到了TimeSort也是优化版的归并排序
   * 3)生成新的迭代器
   */
  def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    //1)所有的kv对移动数组的前端
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))
    //2)利用Sort,KVArraySortDataFormat以及指定的比较器进行排序,
    //由于所有元素都在一个数组里,所以在对这个map里的kv对进行排序时,数组排序的算法在数组内做,节省了内存,效率也比较高
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)
    //3)生成新的迭代器
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }

  /**
   * Return whether the next insert will cause the map to grow
   * 返回插入下一元素是否会导致Map的增长
   */
  def atGrowThreshold: Boolean = curSize == growThreshold
}

private object AppendOnlyMap {
  val MAXIMUM_CAPACITY = (1 << 29) //536870912
}
