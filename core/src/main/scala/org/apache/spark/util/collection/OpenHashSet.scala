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

import scala.reflect._
import com.google.common.hash.Hashing

import org.apache.spark.annotation.Private

/**
 * A simple, fast hash set optimized for non-null insertion-only use case, where keys are never
 * removed.
 *  一个简单,快速的散列集优化用于非空插入用例，其中键永远不会被删除。
  *
 * The underlying implementation uses Scala compiler's specialization to generate optimized
 * storage for two primitive types (Long and Int). It is much faster than Java's standard HashSet
 * while incurring much less memory overhead. This can serve as building blocks for higher level
 * data structures such as an optimized HashMap.
 * 底层实现使用Scala编译器的专门化来为两个原始类型（long和Int）生成优化存储。
  * 它比java的标准好而招致更少的内存开销,这可以作为更高级别的数据结构如一个优化的HashMap积木。
  *
 * This OpenHashSet is designed to serve as building blocks for higher level data structures
 * such as an optimized hash map. Compared with standard hash set implementations, this class
 * provides its various callbacks interfaces (e.g. allocateFunc, moveFunc) and interfaces to
 * retrieve the position of a key in the underlying array.
 * 这openhashset旨在作为更高级别的数据结构如一个优化的哈希映射的积木。与标准的散列集的实现方式相比，
  * 该类提供了不同的回调接口（例如allocatefunc，movefunc）和接口来检索在底层数组的键的位置。
  *
 * It uses quadratic probing with a power-of-2 hash table size, which is guaranteed
 * to explore all spaces for each key (see http://en.wikipedia.org/wiki/Quadratic_probing).
  * 它使用一个电源二次探测哈希表的大小,这是保证探索每个关键的所有空间
 */
@Private
class OpenHashSet[@specialized(Long, Int) T: ClassTag](
    initialCapacity: Int,
    loadFactor: Double)
  extends Serializable {

  require(initialCapacity <= OpenHashSet.MAX_CAPACITY,
    s"Can't make capacity bigger than ${OpenHashSet.MAX_CAPACITY} elements")
  require(initialCapacity >= 1, "Invalid initial capacity")
  require(loadFactor < 1.0, "Load factor must be less than 1.0")
  require(loadFactor > 0.0, "Load factor must be greater than 0.0")

  import OpenHashSet._

  def this(initialCapacity: Int) = this(initialCapacity, 0.7)

  def this() = this(64)

  // The following member variables are declared as protected instead of private for the
  // specialization to work (specialized class extends the non-specialized one and needs access
  // to the "private" variables).
  //下列成员变量被声明为受保护而不是私有化,以便专门化工作(专门化的类扩展非专门化的类，需要访问“私有”变量)
  protected val hasher: Hasher[T] = {
    // It would've been more natural to write the following using pattern matching. But Scala 2.9.x
    // compiler has a bug when specialization is used together with this pattern matching, and
    // throws:
    // scala.tools.nsc.symtab.Types$TypeError: type mismatch;
    //  found   : scala.reflect.AnyValManifest[Long]
    //  required: scala.reflect.ClassTag[Int]
    //         at scala.tools.nsc.typechecker.Contexts$Context.error(Contexts.scala:298)
    //         at scala.tools.nsc.typechecker.Infer$Inferencer.error(Infer.scala:207)
    //         ...
    val mt = classTag[T]
    if (mt == ClassTag.Long) {
      (new LongHasher).asInstanceOf[Hasher[T]]
    } else if (mt == ClassTag.Int) {
      (new IntHasher).asInstanceOf[Hasher[T]]
    } else {
      new Hasher[T]
    }
  }

  protected var _capacity = nextPowerOf2(initialCapacity)
  protected var _mask = _capacity - 1
  protected var _size = 0
  protected var _growThreshold = (loadFactor * _capacity).toInt

  protected var _bitset = new BitSet(_capacity)

  def getBitSet: BitSet = _bitset

  // Init of the array in constructor (instead of in declaration) to work around a Scala compiler
  // specialization bug that would generate two arrays (one for Object and one for specialized T).
  //初始化构造函数中的数组（而不是在声明中）来处理一个Scala编译器专门化的bug，它会生成两个数组（一个用于对象，一个用于专门的T）
  protected var _data: Array[T] = _
  _data = new Array[T](_capacity)

  /** Number of elements in the set.
    * 集合中的元素数*/
  def size: Int = _size

  /** The capacity of the set (i.e. size of the underlying array).
    * 集合的容量（即底层数组的大小） */
  def capacity: Int = _capacity

  /** Return true if this set contains the specified element.
    * 如果该集合包含指定元素,则返回true
    *  */
  def contains(k: T): Boolean = getPos(k) != INVALID_POS

  /**
   * Add an element to the set. If the set is over capacity after the insertion, grow the set
   * and rehash all elements.
    * 在集合中添加一个元素,如果设置为超过容量插入后,发展集和重复的所有元素
   */
  def add(k: T) {
    addWithoutResize(k)
    rehashIfNeeded(k, grow, move)
  }

  def union(other: OpenHashSet[T]): OpenHashSet[T] = {
    val iterator = other.iterator
    while (iterator.hasNext) {
      add(iterator.next())
    }
    this
  }

  /**
   * Add an element to the set. This one differs from add in that it doesn't trigger rehashing.
   * The caller is responsible for calling rehashIfNeeded.
   * 将一个元素添加到集合中,这一点与添加不同,它不会触发重新散列,调用者负责调用rehashifneeded
   * Use (retval & POSITION_MASK) to get the actual position, and
   * (retval & NONEXISTENCE_MASK) == 0 for prior existence.
   *
   * @return The position where the key is placed, plus the highest order bit is set if the key
   *         does not exists previously.
   */
  def addWithoutResize(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        // This is a new key.
        _data(pos) = k
        _bitset.set(pos)
        _size += 1
        return pos | NONEXISTENCE_MASK
      } else if (_data(pos) == k) {
        // Found an existing key.
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /**
   * Rehash the set if it is overloaded.
    * 如果超载,重新设置。
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  def rehashIfNeeded(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    if (_size > _growThreshold) {
      rehash(k, allocateFunc, moveFunc)
    }
  }

  /**
   * Return the position of the element in the underlying array, or INVALID_POS if it is not found.
    * 返回底层数组中元素的位置，如果没有找到则返回INVALID_POS
   */
  def getPos(k: T): Int = {
    var pos = hashcode(hasher.hash(k)) & _mask
    var delta = 1
    while (true) {
      if (!_bitset.get(pos)) {
        return INVALID_POS
      } else if (k == _data(pos)) {
        return pos
      } else {
        // quadratic probing with values increase by 1, 2, 3, ...
        pos = (pos + delta) & _mask
        delta += 1
      }
    }
    throw new RuntimeException("Should never reach here.")
  }

  /** Return the value at the specified position.
    * 返回指定位置的值
    * */
  def getValue(pos: Int): T = _data(pos)

  def iterator: Iterator[T] = new Iterator[T] {
    var pos = nextPos(0)
    override def hasNext: Boolean = pos != INVALID_POS
    override def next(): T = {
      val tmp = getValue(pos)
      pos = nextPos(pos + 1)
      tmp
    }
  }

  /** Return the value at the specified position.
    * 返回指定位置的值
    * */
  def getValueSafe(pos: Int): T = {
    assert(_bitset.get(pos))
    _data(pos)
  }

  /**
   * Return the next position with an element stored, starting from the given position inclusively.
    * 从包含的给定位置开始,将存储的元素返回下一个位置
   */
  def nextPos(fromPos: Int): Int = _bitset.nextSetBit(fromPos)

  /**
   * Double the table's size and re-hash everything. We are not really using k, but it is declared
   * so Scala compiler can specialize this method (which leads to calling the specialized version
   * of putInto).
    * 将表的大小加倍,并重新排列所有内容。我们不是真的使用k,但是它被声明为Scala编译器可以专门化这种方法(这导致调用专门版本的putInto)
   *
   * @param k A parameter unused in the function, but to force the Scala compiler to specialize
   *          this method.
   * @param allocateFunc Callback invoked when we are allocating a new, larger array.
   * @param moveFunc Callback invoked when we move the key from one position (in the old data array)
   *                 to a new position (in the new data array).
   */
  private def rehash(k: T, allocateFunc: (Int) => Unit, moveFunc: (Int, Int) => Unit) {
    val newCapacity = _capacity * 2
    require(newCapacity > 0 && newCapacity <= OpenHashSet.MAX_CAPACITY,
      s"Can't contain more than ${(loadFactor * OpenHashSet.MAX_CAPACITY).toInt} elements")
    allocateFunc(newCapacity)
    val newBitset = new BitSet(newCapacity)
    val newData = new Array[T](newCapacity)
    val newMask = newCapacity - 1

    var oldPos = 0
    while (oldPos < capacity) {
      if (_bitset.get(oldPos)) {
        val key = _data(oldPos)
        var newPos = hashcode(hasher.hash(key)) & newMask
        var i = 1
        var keepGoing = true
        // No need to check for equality here when we insert so this has one less if branch than
        // the similar code path in addWithoutResize.
        //当我们插入时，不需要检查这里是否平等，所以如果分支比它少一个addWithoutResize中的类似代码路径。
        while (keepGoing) {
          if (!newBitset.get(newPos)) {
            // Inserting the key at newPos
            newData(newPos) = key
            newBitset.set(newPos)
            moveFunc(oldPos, newPos)
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

    _bitset = newBitset
    _data = newData
    _capacity = newCapacity
    _mask = newMask
    _growThreshold = (loadFactor * newCapacity).toInt
  }

  /**
   * Re-hash a value to deal better with hash functions that don't differ in the lower bits.
    * 对一个值进行重新排列以处理较低位中没有差异的散列函数。
   */
  private def hashcode(h: Int): Int = Hashing.murmur3_32().hashInt(h).asInt()

  private def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }
}


private[spark]
object OpenHashSet {

  val MAX_CAPACITY = 1 << 30
  val INVALID_POS = -1
  val NONEXISTENCE_MASK = 1 << 31
  val POSITION_MASK = (1 << 31) - 1

  /**
   * A set of specialized hash function implementation to avoid boxing hash code computation
   * in the specialized implementation of OpenHashSet.
    * 一套专门的哈希函数实现,避免了在专门实现OpenHashSet中的哈希码计算。
   */
  sealed class Hasher[@specialized(Long, Int) T] extends Serializable {
    def hash(o: T): Int = o.hashCode()
  }

  class LongHasher extends Hasher[Long] {
    override def hash(o: Long): Int = (o ^ (o >>> 32)).toInt
  }

  class IntHasher extends Hasher[Int] {
    override def hash(o: Int): Int = o
  }

  private def grow1(newSize: Int) {}
  private def move1(oldPos: Int, newPos: Int) { }

  private val grow = grow1 _
  private val move = move1 _
}
