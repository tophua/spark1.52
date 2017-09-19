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

package org.apache.spark.network.shuffle;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Wraps another BlockFetcher with the ability to automatically retry fetches which fail due to
 * IOExceptions, which we hope are due to transient network conditions.
 *
 * 封装另一个BlockFetcher,可以自动重试由于IOExceptions失败的提取,我们希望这是由于瞬态网络条件。
 *
 * This fetcher provides stronger guarantees regarding the parent BlockFetchingListener. In
 * particular, the listener will be invoked exactly once per blockId, with a success or failure.
 * 此提取器对父级BlockFetchingListener提供了更强的保证,具体来说,每个blockId将调用一次,成功或失败,
 */
public class RetryingBlockFetcher {

  /**
   * Used to initiate the first fetch for all blocks, and subsequently for retrying the fetch on any
   * remaining blocks.
   * 用于启动所有块的第一次抓取，然后用于重试任何剩余块上的抓取
   */
  public static interface BlockFetchStarter {
    /**
     * Creates a new BlockFetcher to fetch the given block ids which may do some synchronous
     * bootstrapping followed by fully asynchronous block fetching.
     * 创建一个新的BlockFetcher来获取给定的块ID,可以进行一些同步引导,然后完全异步块提取,
     *
     * The BlockFetcher must eventually invoke the Listener on every input blockId, or else this
     * method must throw an exception.
     * BlockFetcher必须最终在每个输入blockId上调用侦听器，否则该方法必须抛出异常
     *
     * This method should always attempt to get a new TransportClient from the
     * 此方法应始终尝试从中获取新的TransportClient
     * {@link org.apache.spark.network.client.TransportClientFactory} in order to fix connection
     * issues.
     */
    void createAndStart(String[] blockIds, BlockFetchingListener listener) throws IOException;
  }

  /** Shared executor service used for waiting and retrying. */
  //SynchronousQueue：一个不存储元素的阻塞队列。每个插入操作必须等到另一个线程调用移除操作，否则插入操作一直处于阻塞状态，
  //吞吐量通常要高于LinkedBlockingQueue，静态工厂方法Executors.newCachedThreadPool使用了这个队列。
  private static final ExecutorService executorService = Executors.newCachedThreadPool(
    NettyUtils.createThreadFactory("Block Fetch Retry"));

  private final Logger logger = LoggerFactory.getLogger(RetryingBlockFetcher.class);

  /** Used to initiate new Block Fetches on our remaining blocks.
   * 用于在我们的剩余块上启动新的块提取*/
  private final BlockFetchStarter fetchStarter;

  /** Parent listener which we delegate all successful or permanently failed block fetches to.
   * 我们委托所有成功或永久失败的块提取的父监听器*/
  private final BlockFetchingListener listener;

  /**
   *  Max number of times we are allowed to retry.
   *  最大允许重试 次数
   * */
  private final int maxRetries;

  /**
   *  Milliseconds to wait before each retry.
   *  每次重试之前等待的毫秒数 
   * */
  private final int retryWaitTime;

  // NOTE:
  // All of our non-final fields are synchronized under 'this' and should only be accessed/mutated
  // while inside a synchronized block. 我们的所有非最终字段都在“this”下进行同步,并且只能在同步块内部进行访问/变更
  //目前为止尝试次数
  /** Number of times we've attempted to retry so far.
   * 我们到目前为止尝试重试的次数。*/
  private int retryCount = 0;

  /**
   * Set of all block ids which have not been fetched successfully or with a non-IO Exception.
   * A retry involves requesting every outstanding block. Note that since this is a LinkedHashSet,
   * input ordering is preserved, so we always request blocks in the same order the user provided.
   *
   * 设置所有未成功获取的块ID或非IO异常,重试涉及请求每个未完成的块,
   * 请注意,由于这是一个LinkedHashSet,所以输入顺序被保留,所以我们总是按照用户提供的顺序请求块
   */
  private final LinkedHashSet<String> outstandingBlocksIds;

  /**
   * The BlockFetchingListener that is active with our current BlockFetcher.
   * 使用我们当前的BlockFetcher活动的BlockFetchingListener。
   * When we start a retry, we immediately replace this with a new Listener, which causes all any
   * old Listeners to ignore all further responses.
   * 当我们开始重试时,我们立即用一个新的Listener替换它,这会导致所有旧的Listener忽略所有进一步的响应。
   */
  private RetryingBlockFetchListener currentListener;

  public RetryingBlockFetcher(
      TransportConf conf,
      BlockFetchStarter fetchStarter,
      String[] blockIds,
      BlockFetchingListener listener) {
    this.fetchStarter = fetchStarter;
    this.listener = listener;
    this.maxRetries = conf.maxIORetries();
    this.retryWaitTime = conf.ioRetryWaitTimeMs();
    this.outstandingBlocksIds = Sets.newLinkedHashSet();
    Collections.addAll(outstandingBlocksIds, blockIds);
    this.currentListener = new RetryingBlockFetchListener();
  }

  /**
   * Initiates the fetch of all blocks provided in the constructor, with possible retries in the
   * event of transient IOExceptions.
   * 启动获取构造函数中提供的所有块,并在发生瞬态IOExceptions的情况下进行可能的重试。
   */
  public void start() {
    fetchAllOutstanding();
  }

  /**
   * Fires off a request to fetch all blocks that have not been fetched successfully or permanently
   * failed (i.e., by a non-IOException).
   * 触发请求以获取未成功获取或永久失败的所有块（即非IOException）
   */
  private void fetchAllOutstanding() {
    // Start by retrieving our shared state within a synchronized block.
      //首先在同步块中检索我们的共享状态
    String[] blockIdsToFetch;
    int numRetries;
    RetryingBlockFetchListener myListener;
    synchronized (this) {
      blockIdsToFetch = outstandingBlocksIds.toArray(new String[outstandingBlocksIds.size()]);
      numRetries = retryCount;
      myListener = currentListener;
    }

    // Now initiate the fetch on all outstanding blocks, possibly initiating a retry if that fails.
      //现在启动所有未完成的块的抓取，如果失败，可能会启动重试
    try {
      fetchStarter.createAndStart(blockIdsToFetch, myListener);
    } catch (Exception e) {
      logger.error(String.format("Exception while beginning fetch of %s outstanding blocks %s",
        blockIdsToFetch.length, numRetries > 0 ? "(after " + numRetries + " retries)" : ""), e);

      if (shouldRetry(e)) {
        initiateRetry();
      } else {
        for (String bid : blockIdsToFetch) {
          listener.onBlockFetchFailure(bid, e);
        }
      }
    }
  }

  /**
   * Lightweight method which initiates a retry in a different thread. The retry will involve
   * calling fetchAllOutstanding() after a configured wait time.
   * 在不同线程中启动重试的轻量级方法,重试将涉及在配置的等待时间后调用fetchAllOutstanding()
   */
  private synchronized void initiateRetry() {
    retryCount += 1;
    currentListener = new RetryingBlockFetchListener();

    logger.info("Retrying fetch ({}/{}) for {} outstanding blocks after {} ms",
      retryCount, maxRetries, outstandingBlocksIds.size(), retryWaitTime);

    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Uninterruptibles.sleepUninterruptibly(retryWaitTime, TimeUnit.MILLISECONDS);
        fetchAllOutstanding();
      }
    });
  }

  /**
   * Returns true if we should retry due a block fetch failure. We will retry if and only if
   * the exception was an IOException and we haven't retried 'maxRetries' times already.
   * 如果我们应该重试执行块提取失败，则返回true,
   * 当且仅当异常为IOException且我们尚未重试“maxRetries”时，我们将重试。
   */
  private synchronized boolean shouldRetry(Throwable e) {
    boolean isIOException = e instanceof IOException
      || (e.getCause() != null && e.getCause() instanceof IOException);
    boolean hasRemainingRetries = retryCount < maxRetries;
    return isIOException && hasRemainingRetries;
  }

  /**
   * Our RetryListener intercepts block fetch responses and forwards them to our parent listener.
   * Note that in the event of a retry, we will immediately replace the 'currentListener' field,
   * indicating that any responses from non-current Listeners should be ignored.
   * 我们的RetryListener拦截块提取响应并将其转发给我们的父监听器,
   * 注意，在重试的情况下，我们将立即替换'currentListener'字段，表示应该忽略来自非当前侦听器的任何响应。
   */
  private class RetryingBlockFetchListener implements BlockFetchingListener {
    @Override
    public void onBlockFetchSuccess(String blockId, ManagedBuffer data) {
      // We will only forward this success message to our parent listener if this block request is
      // outstanding and we are still the active listener.
        //如果这个阻塞请求是未完成的,而且我们仍然是主动侦听器,我们只会将这个成功消息转发给我们的父监听者,
      boolean shouldForwardSuccess = false;
      synchronized (RetryingBlockFetcher.this) {
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          outstandingBlocksIds.remove(blockId);
          shouldForwardSuccess = true;
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
        //现在实际上调用了同步块外的父监听器
      if (shouldForwardSuccess) {
        listener.onBlockFetchSuccess(blockId, data);
      }
    }

    @Override
    public void onBlockFetchFailure(String blockId, Throwable exception) {
      // We will only forward this failure to our parent listener if this block request is
      // outstanding, we are still the active listener, AND we cannot retry the fetch.
        //我们只会将此失败转发给我们的父级监听器，如果该阻塞请求未完成，我们仍然是主动侦听器，而且我们无法重试提取。
      boolean shouldForwardFailure = false;
      synchronized (RetryingBlockFetcher.this) {
        if (this == currentListener && outstandingBlocksIds.contains(blockId)) {
          if (shouldRetry(exception)) {
            initiateRetry();
          } else {
            logger.error(String.format("Failed to fetch block %s, and will not retry (%s retries)",
              blockId, retryCount), exception);
            outstandingBlocksIds.remove(blockId);
            shouldForwardFailure = true;
          }
        }
      }

      // Now actually invoke the parent listener, outside of the synchronized block.
        //现在实际上调用了同步块外的父监听器
      if (shouldForwardFailure) {
        listener.onBlockFetchFailure(blockId, exception);
      }
    }
  }
}
