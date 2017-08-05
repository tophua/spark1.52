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

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 * 连续的内存块,从固定大小的{@link MemoryLocation}开始
 */
public class MemoryBlock extends MemoryLocation {

  private final long length;

  /**
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * MemoryManager. This is package-private and is modified by MemoryManager.
   * 可选页码 当MemoryBlock表示由MemoryManager分配的页面时使用,这是包私有的,由MemoryManager修改
   */
  int pageNumber = -1;

  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * Returns the size of the memory block.
   * 返回内存块的大小
   */
  public long size() {
    return length;
  }

  /**
   * Creates a memory block pointing to the memory used by the long array.
   * 创建指向长阵列使用的内存的内存块
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8);
  }
}
