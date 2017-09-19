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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.util.SystemPropertyConfigProvider;
import org.apache.spark.network.util.TransportConf;
import static org.apache.spark.network.shuffle.RetryingBlockFetcher.BlockFetchStarter;

/**
 * Tests retry logic by throwing IOExceptions and ensuring that subsequent attempts are made to
 * fetch the lost blocks.
 * 测试通过投掷ioexceptions重试逻辑,确保后续尝试获取丢失的块
 */
public class RetryingBlockFetcherSuite {

  ManagedBuffer block0 = new NioManagedBuffer(ByteBuffer.wrap(new byte[13]));
  ManagedBuffer block1 = new NioManagedBuffer(ByteBuffer.wrap(new byte[7]));
  ManagedBuffer block2 = new NioManagedBuffer(ByteBuffer.wrap(new byte[19]));

  @Before
  public void beforeEach() {
    System.setProperty("spark.shuffle.io.maxRetries", "2");
    System.setProperty("spark.shuffle.io.retryWait", "0");
  }

  @After
  public void afterEach() {
    System.clearProperty("spark.shuffle.io.maxRetries");
    System.clearProperty("spark.shuffle.io.retryWait");
  }

  @Test
  public void testNoFailures() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // Immediately return both blocks successfully.
      //立即返回两个块成功
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", block1)
        .build()
      );

    performInteractions(interactions, listener);

    verify(listener).onBlockFetchSuccess("b0", block0);
    verify(listener).onBlockFetchSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testUnrecoverableFailure() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0 throws a non-IOException error, so it will be failed without retry.
      //B0抛出一个非IOException错误,就没有重试
      ImmutableMap.<String, Object>builder()
        .put("b0", new RuntimeException("Ouch!"))
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener).onBlockFetchFailure(eq("b0"), (Throwable) any());
    verify(listener).onBlockFetchSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testSingleIOExceptionOnFirst() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // IOException will cause a retry. Since b0 fails, we will retry both.
      //IOException会重试,由于b0失败,我们都将重试
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException("Connection failed or something"))
        .put("b1", block1)
        .build(),
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testSingleIOExceptionOnSecond() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // IOException will cause a retry. Since b1 fails, we will not retry b0.
      //IOException会重试,由于B1失败,我们不会重试B0
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new IOException("Connection failed or something"))
        .build(),
      ImmutableMap.<String, Object>builder()
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testTwoIOExceptions() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0's IOException will trigger retry, b1's will be ignored.
      //B0的IOException将触发重试,B1的将被忽略
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException())
        .put("b1", new IOException())
        .build(),
      // Next, b0 is successful and b1 errors again, so we just request that one.
      //接下来,B0和B1的错误又是成功的,所以我们只是要求一个。
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new IOException())
        .build(),
      // b1 returns successfully within 2 retries.
      //B1成功返回,在2次重试
      ImmutableMap.<String, Object>builder()
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockFetchSuccess("b1", block1);
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testThreeIOExceptions() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0's IOException will trigger retry, b1's will be ignored.
      //B0的IOException将触发重试,B1的将被忽略
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException())
        .put("b1", new IOException())
        .build(),
      // Next, b0 is successful and b1 errors again, so we just request that one.
      //接下来,B0和B1的错误又是成功的,所以我们只是要求一个
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new IOException())
        .build(),
      // b1 errors again, but this was the last retry
      //B1的错误了,但这是最后的重试
      ImmutableMap.<String, Object>builder()
        .put("b1", new IOException())
        .build(),
      // This is not reached -- b1 has failed.
      //这是不是达到B1失败
      ImmutableMap.<String, Object>builder()
        .put("b1", block1)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockFetchFailure(eq("b1"), (Throwable) any());
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testRetryAndUnrecoverable() throws IOException {
    BlockFetchingListener listener = mock(BlockFetchingListener.class);

    List<? extends Map<String, Object>> interactions = Arrays.asList(
      // b0's IOException will trigger retry, subsequent messages will be ignored.
      //b0的IOException将触发重试,后续消息将被忽略
      ImmutableMap.<String, Object>builder()
        .put("b0", new IOException())
        .put("b1", new RuntimeException())
        .put("b2", block2)
        .build(),
      // Next, b0 is successful, b1 errors unrecoverably, and b2 triggers a retry.
      //接下来,B0是成功的,B1和B2触发错误并无法恢复,重试
      ImmutableMap.<String, Object>builder()
        .put("b0", block0)
        .put("b1", new RuntimeException())
        .put("b2", new IOException())
        .build(),
      // b2 succeeds in its last retry.
      //B2成功地在其上重试
      ImmutableMap.<String, Object>builder()
        .put("b2", block2)
        .build()
    );

    performInteractions(interactions, listener);

    verify(listener, timeout(5000)).onBlockFetchSuccess("b0", block0);
    verify(listener, timeout(5000)).onBlockFetchFailure(eq("b1"), (Throwable) any());
    verify(listener, timeout(5000)).onBlockFetchSuccess("b2", block2);
    verifyNoMoreInteractions(listener);
  }

  /**
   * Performs a set of interactions in response to block requests from a RetryingBlockFetcher.
   * 执行响应块从retryingblockfetcher要求一组相互作用
   * Each interaction is a Map from BlockId to either ManagedBuffer or Exception. This interaction
   * 每一次的互动是从BlockId到ManagedBuffer或异常图
   * means "respond to the next block fetch request with these Successful buffers and these Failure
   * exceptions". We verify that the expected block ids are exactly the ones requested.
   *
   * If multiple interactions are supplied, they will be used in order. This is useful for encoding
   * retries -- the first interaction may include an IOException, which causes a retry of some
   * subset of the original blocks in a second interaction.
   */
  @SuppressWarnings("unchecked")
  private static void performInteractions(List<? extends Map<String, Object>> interactions,
                                          BlockFetchingListener listener)
    throws IOException {

    TransportConf conf = new TransportConf(new SystemPropertyConfigProvider());
    BlockFetchStarter fetchStarter = mock(BlockFetchStarter.class);

    Stubber stub = null;

    // Contains all blockIds that are referenced across all interactions.
    //包含所有blockids进行引用,所有的相互作用
    final LinkedHashSet<String> blockIds = Sets.newLinkedHashSet();

    for (final Map<String, Object> interaction : interactions) {
      blockIds.addAll(interaction.keySet());

      Answer<Void> answer = new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
          try {
            // Verify that the RetryingBlockFetcher requested the expected blocks.
        	//验证retryingblockfetcher要求预期的块
            String[] requestedBlockIds = (String[]) invocationOnMock.getArguments()[0];
            String[] desiredBlockIds = interaction.keySet().toArray(new String[interaction.size()]);
            assertArrayEquals(desiredBlockIds, requestedBlockIds);

            // Now actually invoke the success/failure callbacks on each block.
            //现在实际上调用成功/失败回调的每一块
            BlockFetchingListener retryListener =
              (BlockFetchingListener) invocationOnMock.getArguments()[1];
            for (Map.Entry<String, Object> block : interaction.entrySet()) {
              String blockId = block.getKey();
              Object blockValue = block.getValue();

              if (blockValue instanceof ManagedBuffer) {
                retryListener.onBlockFetchSuccess(blockId, (ManagedBuffer) blockValue);
              } else if (blockValue instanceof Exception) {
                retryListener.onBlockFetchFailure(blockId, (Exception) blockValue);
              } else {
                fail("Can only handle ManagedBuffers and Exceptions, got " + blockValue);
              }
            }
            return null;
          } catch (Throwable e) {
            e.printStackTrace();
            throw e;
          }
        }
      };

      // This is either the first stub, or should be chained behind the prior ones.
      //这是第一个存根,或应该被链接在前面的
      if (stub == null) {
        stub = doAnswer(answer);
      } else {
        stub.doAnswer(answer);
      }
    }

    assert stub != null;
    stub.when(fetchStarter).createAndStart((String[]) any(), (BlockFetchingListener) anyObject());
    String[] blockIdArray = blockIds.toArray(new String[blockIds.size()]);
    new RetryingBlockFetcher(conf, fetchStarter, blockIdArray, listener).start();
  }
}
