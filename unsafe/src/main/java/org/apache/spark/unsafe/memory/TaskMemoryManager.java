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

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the memory allocated by an individual task.
 * 管理单个任务分配的内存
 * <p>
 * Most of the complexity in this class deals with encoding of off-heap addresses into 64-bit longs.
 * In off-heap mode, memory can be directly addressed with 64-bit longs. In on-heap mode, memory is
 * addressed by the combination of a base Object reference and a 64-bit offset within that object.
 * This is a problem when we want to store pointers to data structures inside of other structures,
 * such as record pointers inside hashmaps or sorting buffers. Even if we decided to use 128 bits
 * to address memory, we can't just store the address of the base object since it's not guaranteed
 * to remain stable as the heap gets reorganized due to GC.
 * 这个类中的大部分复杂性都是将堆栈内地址编码为64位长的数据,在非堆栈模式下，内存可以直接寻址64位长。
 * 在堆模式下，内存是通过基础对象引用和该对象内的64位偏移的组合来寻址。
 *这是一个问题，当我们想存储指向其他结构内的数据结构的指针时，如hashmaps或排序缓冲区中的记录指针。
 * 即使我们决定使用128位为了解决内存，我们不能仅仅存储基础对象的地址，
 * 因为它不能保证由于GC由于堆重组而保持稳定。
 * <p>
 * Instead, we use the following approach to encode record pointers in 64-bit longs: for off-heap
 * mode, just store the raw address, and for on-heap mode use the upper 13 bits of the address to
 * store a "page number" and the lower 51 bits to store an offset within this page. These page
 * numbers are used to index into a "page table" array inside of the MemoryManager in order to
 * retrieve the base object.
 * 相反,我们使用以下方法对64位长的记录指针进行编码：对于非堆模式,只存储原始地址,对于堆栈模式，
   使用地址的高13位存储一个“页码”,较低的51位存储该页面中的偏移量。 这些页面数字用于索引到MemoryManager内的“页表”数组检索基础对象。
 * <p>
 * This allows us to address 8192 pages. In on-heap mode, the maximum page size is limited by the
 * maximum size of a long[] array, allowing us to address 8192 * 2^32 * 8 bytes, which is
 * approximately 35 terabytes of memory.
 *
 * 这使我们能够处理8192页,在堆栈模式下,最大页面大小受长[]数组的最大大小限制,允许我们寻址8192 * 2 ^ 32 * 8字节,大约35 TB的内存。
 * 
 * TaskMemoryManager负责每个task的内存分配和跟踪。它通过page table追踪on-heap内存块，
 * task退出时它若检查到有page未释放则会抛出异常。
 * 它使用ExecutorMemoryManager真正执行分配和释放。每个task一个实例。
 * 
 * 这几个类如下进行交互:
 * 一旦task需要分配较大内存，它首先向ShuffleMemoryManager申请X bytes。如果申请可以被满足，
 * task会向TaskMemoryManager要求分配X bytes内存。后者更新自己的page table，
 * 并且要求ExecutorMemoryManager真正分配内存
 */
public class TaskMemoryManager {

  private final Logger logger = LoggerFactory.getLogger(TaskMemoryManager.class);

  /** The number of bits used to address the page table.
   * 用于寻址页表的位数 */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of bits used to encode offsets in data pages.
   * 用于对数据页中的偏移量进行编码的位数 */
  @VisibleForTesting
  static final int OFFSET_BITS = 64 - PAGE_NUMBER_BITS;  // 51

  /** The number of entries in the page table.页表中的条目数 */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  /**
   * Maximum supported data page size (in bytes). In principle, the maximum addressable page size is
   * (1L &lt;&lt; OFFSET_BITS) bytes, which is 2+ petabytes. However, the on-heap allocator's maximum page
   * size is limited by the maximum amount of data that can be stored in a  long[] array, which is
   * (2^32 - 1) * 8 bytes (or 16 gigabytes). Therefore, we cap this at 16 gigabytes.
   *
   * 最大支持的数据页大小（以字节为单位）。 原则上，最大可寻址页面大小是（1L＆lt; OFFSET_BITS）字节,它是2PB字节。
   * 但是,堆内分配器的最大页大小受限于长[]数组（即2 ^ 32 - 1）* 8字节（或16千兆字节））的最大数据量。
   * 所以我们把它限制在16G字节。
   */
  public static final long MAXIMUM_PAGE_SIZE_BYTES = ((1L << 31) - 1) * 8L;

  /** Bit mask for the lower 51 bits of a long.
   * 位长掩码为低51位*/
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  /** Bit mask for the upper 13 bits of a long
   * 位长度为13位的位掩码*/
  private static final long MASK_LONG_UPPER_13_BITS = ~MASK_LONG_LOWER_51_BITS;

  /**
   * Similar to an operating system's page table, this array maps page numbers into base object
   * pointers, allowing us to translate between the hashtable's internal 64-bit address
   * representation and the baseObject+offset representation which we use to support both in- and
   * off-heap addresses. When using an off-heap allocator, every entry in this map will be `null`.
   * When using an in-heap allocator, the entries in this map will point to pages' base objects.
   * Entries are added to this map as new data pages are allocated.
   * 与操作系统的页表类似，该数组将页码映射到基对象指针,允许我们在哈希表的内部64位地址之间进行转换
   * 表示和baseObject + offset表示,我们用来支持in和非堆地址。 当使用非堆分配器时,此映射中的每个条目都将为“null”。
   *当使用堆内分配器时,此映射中的条目将指向页面的基础对象,新数据页被分配时,条目将添加到该地图。
   */
  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];

  /**
   * Bitmap for tracking free pages.
   * 跟踪免费页面的位图
   */
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  /**
   * Tracks memory allocated with {@link TaskMemoryManager#allocate(long)}, used to detect / clean
   * up leaked memory.
   * 跟踪用{@link TaskMemoryManager＃allocate（long）}分配的内存,用于检测/清除泄露的内存
   */
  private final HashSet<MemoryBlock> allocatedNonPageMemory = new HashSet<MemoryBlock>();

  private final ExecutorMemoryManager executorMemoryManager;

  /**
   * Tracks whether we're in-heap or off-heap. For off-heap, we short-circuit most of these methods
   * without doing any masking or lookups. Since this branching should be well-predicted by the JIT,
   * this extra layer of indirection / abstraction hopefully shouldn't be too expensive.
   *
   * 跟踪我们是否在堆或堆内,对于非堆,我们短路大多数这些方法,而不进行任何屏蔽或查找,
   * 由于这种分支应该由JIT很好地预测,所以这个额外的间接/抽象层希望不应太贵。
   */
  private final boolean inHeap;

  /**
   * Construct a new MemoryManager.
   */
  public TaskMemoryManager(ExecutorMemoryManager executorMemoryManager) {
    this.inHeap = executorMemoryManager.inHeap;
    this.executorMemoryManager = executorMemoryManager;
  }

  /**
   * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
   * intended for allocating large blocks of memory that will be shared between operators.
   * 分配将在MemoryManager的页表中跟踪的一块内存,这是为了分配将在运营商之间共享的大块内存
   */
  public MemoryBlock allocatePage(long size) {
    if (size > MAXIMUM_PAGE_SIZE_BYTES) {
      throw new IllegalArgumentException(
        "Cannot allocate a page with more than " + MAXIMUM_PAGE_SIZE_BYTES + " bytes");
    }

    final int pageNumber;
    synchronized (this) {
      pageNumber = allocatedPages.nextClearBit(0);
      if (pageNumber >= PAGE_TABLE_SIZE) {
        throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
      }
      allocatedPages.set(pageNumber);
    }
    final MemoryBlock page = executorMemoryManager.allocate(size);
    page.pageNumber = pageNumber;
    pageTable[pageNumber] = page;
    if (logger.isTraceEnabled()) {
      logger.trace("Allocate page number {} ({} bytes)", pageNumber, size);
    }
    return page;
  }

  /**
   * Free a block of memory allocated via {@link TaskMemoryManager#allocatePage(long)}.
   * 释放通过{@link TaskMemoryManager＃allocatePage（long）}分配的内存块
   */
  public void freePage(MemoryBlock page) {
    assert (page.pageNumber != -1) :
      "Called freePage() on memory that wasn't allocated with allocatePage()";
    assert(allocatedPages.get(page.pageNumber));
    pageTable[page.pageNumber] = null;
    synchronized (this) {
      allocatedPages.clear(page.pageNumber);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("Freed page number {} ({} bytes)", page.pageNumber, page.size());
    }
    // Cannot access a page once it's freed.
      //释放页面后无法访问该页面
    executorMemoryManager.free(page);
  }

  /**
   * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
   * to be zeroed out (call `zero()` on the result if this is necessary). This method is intended
   * to be used for allocating operators' internal data structures. For data pages that you want to
   * exchange between operators, consider using {@link TaskMemoryManager#allocatePage(long)}, since
   * that will enable intra-memory pointers (see
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)} and this class's
   * top-level Javadoc for more details).
   *
   * 分配一个连续的内存块。 请注意，分配的内存不能保证被清零（如果需要，则在结果上调用`zero（）`）。
   * 该方法旨在用于分配运算符的内部数据结构。 对于要在运算符之间交换的数据页面，
   * 请考虑使用{@link TaskMemoryManager＃allocatePage（long）}，因为这将启用内存内指针（
   * 请参阅{@link TaskMemoryManager＃encodePageNumberAndOffset（MemoryBlock，long）}和此类的顶部 级Javadoc了解更多细节）
   */
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    assert(size > 0) : "Size must be positive, but got " + size;
    final MemoryBlock memory = executorMemoryManager.allocate(size);
    synchronized(allocatedNonPageMemory) {
      allocatedNonPageMemory.add(memory);
    }
    return memory;
  }

  /**
   * Free memory allocated by {@link TaskMemoryManager#allocate(long)}.
   * 由{@link TaskMemoryManager＃allocate（long）}分配的空闲内存
   */
  public void free(MemoryBlock memory) {
    assert (memory.pageNumber == -1) : "Should call freePage() for pages, not free()";
    executorMemoryManager.free(memory);
    synchronized(allocatedNonPageMemory) {
      final boolean wasAlreadyRemoved = !allocatedNonPageMemory.remove(memory);
      assert (!wasAlreadyRemoved) : "Called free() on memory that was already freed!";
    }
  }

  /**
   * Given a memory page and offset within that page, encode this address into a 64-bit long.
   * This address will remain valid as long as the corresponding page has not been freed.
   * 给定该页面内存页面和偏移量,将该地址编码为64位长。只要相应页面没有被释放,该地址将保持有效,
   *
   * @param page a data page allocated by {@link TaskMemoryManager#allocate(long)}.
   * @param offsetInPage an offset in this page which incorporates the base offset. In other words,
   *                     this should be the value that you would pass as the base offset into an
   *                     UNSAFE call (e.g. page.baseOffset() + something).
   * @return an encoded page address.
   */
  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    if (!inHeap) {
      // In off-heap mode, an offset is an absolute address that may require a full 64 bits to
      // encode. Due to our page size limitation, though, we can convert this into an offset that's
      // relative to the page's base offset; this relative offset will fit in 51 bits.
      offsetInPage -= page.getBaseOffset();
    }
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage);
  }

  @VisibleForTesting
  public static long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber != -1) : "encodePageNumberAndOffset called with invalid page";
    return (((long) pageNumber) << OFFSET_BITS) | (offsetInPage & MASK_LONG_LOWER_51_BITS);
  }

  @VisibleForTesting
  public static int decodePageNumber(long pagePlusOffsetAddress) {
    return (int) ((pagePlusOffsetAddress & MASK_LONG_UPPER_13_BITS) >>> OFFSET_BITS);
  }

  private static long decodeOffset(long pagePlusOffsetAddress) {
    return (pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS);
  }

  /**
   * Get the page associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   * 获取与编码的地址相关联的页面
   * {@link TaskMemoryManager＃encodePageNumberAndOffset（MemoryBlock，long）}
   */
  public Object getPage(long pagePlusOffsetAddress) {
    if (inHeap) {
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      assert (page.getBaseObject() != null);
      return page.getBaseObject();
    } else {
      return null;
    }
  }

  /**
   * Get the offset associated with an address encoded by
   * {@link TaskMemoryManager#encodePageNumberAndOffset(MemoryBlock, long)}
   * 获取与编码的地址相关联的偏移量
   * {@link TaskMemoryManager＃encodePageNumberAndOffset（MemoryBlock，long）}
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    final long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    if (inHeap) {
      return offsetInPage;
    } else {
      // In off-heap mode, an offset is an absolute address. In encodePageNumberAndOffset, we
      // converted the absolute address into a relative address. Here, we invert that operation:
        //在非堆模式下，偏移量是绝对地址。 在encodePageNumberAndOffset中，我们
        //将绝对地址转换为相对地址。 在这里，我们反转了这个操作：
      final int pageNumber = decodePageNumber(pagePlusOffsetAddress);
      assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
      final MemoryBlock page = pageTable[pageNumber];
      assert (page != null);
      return page.getBaseOffset() + offsetInPage;
    }
  }

  /**
   * Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return
   * value can be used to detect memory leaks.
   * 清理所有分配的内存和页面,返回释放的字节数,可以使用非零返回值来检测内存泄漏
   */
  public long cleanUpAllAllocatedMemory() {
    long freedBytes = 0;
    for (MemoryBlock page : pageTable) {
      if (page != null) {
        freedBytes += page.size();
        freePage(page);
      }
    }

    synchronized (allocatedNonPageMemory) {
      final Iterator<MemoryBlock> iter = allocatedNonPageMemory.iterator();
      while (iter.hasNext()) {
        final MemoryBlock memory = iter.next();
        freedBytes += memory.size();
        // We don't call free() here because that calls Set.remove, which would lead to a
        // ConcurrentModificationException here.
          //我们不会在这里调用free(),因为它调用Set.remove,这将导致ConcurrentModificationException在这里
        executorMemoryManager.free(memory);
        iter.remove();
      }
    }
    return freedBytes;
  }
}
