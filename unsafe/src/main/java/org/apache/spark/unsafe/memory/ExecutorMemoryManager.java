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

package org.apache.spark.unsafe.memory;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;

/**
 * Manages memory for an executor. Individual operators / tasks allocate memory through
 * {@link TaskMemoryManager} objects, which obtain their memory from ExecutorMemoryManager.
 * 
 * 负责具体实现，它管理on-heap和off-heap内存，并实现了一个weak reference pool支持跨tasks的空闲页复用。
 * 一个JVM里仅有一个实例
 */
public class ExecutorMemoryManager {

  /**
   * Allocator, exposed for enabling untracked allocations of temporary data structures.
   * 分配器,用于启用临时数据结构的未跟踪分配
   */
  public final MemoryAllocator allocator;

  /**
   * Tracks whether memory will be allocated on the JVM heap or off-heap using sun.misc.Unsafe.
   * 跟踪内存是否将使用sun.misc.Unsafe在JVM堆或非堆上分配
   */
  final boolean inHeap;

  @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<MemoryBlock>>> bufferPoolsBySize =
    new HashMap<Long, LinkedList<WeakReference<MemoryBlock>>>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Construct a new ExecutorMemoryManager.
   * 构造一个新的ExecutorMemoryManager
   *
   * @param allocator the allocator that will be used
   */
  public ExecutorMemoryManager(MemoryAllocator allocator) {
    this.inHeap = allocator instanceof HeapMemoryAllocator;
    this.allocator = allocator;
  }

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   * 如果给定大小的分配应通过池化机制,则返回true,否则返回false
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
      //非常小的分配不太可能从池中获益
    // At some point, we should explore supporting pooling for off-heap memory, but for now we'll
    // ignore that case in the interest of simplicity.
      //在某些时候,我们应该探索支持堆栈内存的方式,但是现在我们就为了简单起见而忽视这种情况
    return size >= POOLING_THRESHOLD_BYTES && allocator instanceof HeapMemoryAllocator;
  }

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary).
   * 分配一个连续的内存块。请注意,分配的内存不能保证被清零(如果需要，则在结果上调用`zero()`)
   */
  MemoryBlock allocate(long size) throws OutOfMemoryError {
    if (shouldPool(size)) {
      synchronized (this) {
        final LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<MemoryBlock> blockReference = pool.pop();
            final MemoryBlock memory = blockReference.get();
            if (memory != null) {
              assert (memory.size() == size);
              return memory;
            }
          }
          bufferPoolsBySize.remove(size);
        }
      }
      return allocator.allocate(size);
    } else {
      return allocator.allocate(size);
    }
  }

  void free(MemoryBlock memory) {
    final long size = memory.size();
    if (shouldPool(size)) {
      synchronized (this) {
        LinkedList<WeakReference<MemoryBlock>> pool = bufferPoolsBySize.get(size);
        if (pool == null) {
          pool = new LinkedList<WeakReference<MemoryBlock>>();
          bufferPoolsBySize.put(size, pool);
        }
        pool.add(new WeakReference<MemoryBlock>(memory));
      }
    } else {
      allocator.free(memory);
    }
  }

}
