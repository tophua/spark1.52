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

package org.apache.spark.network.server;

import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.util.NettyUtils;

/**
 * A handler that processes requests from clients and writes chunk data back. Each handler is
 * attached to a single Netty channel, and keeps track of which streams have been fetched via this
 * channel, in order to clean them up if the channel is terminated (see #channelUnregistered).
 *
 * 处理来自客户端的请求并将块数据写回的处理程序,每个处理程序都连接到一个Netty通道,并跟踪通过此通道获取的流,
 * 以便在通道终止时清除它们（请参阅#channel注册）。
 *
 * The messages should have been processed by the pipeline setup by {@link TransportServer}.
 * 这些消息应该由{@link TransportServer}的流水线设置处理
 */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
  private final Logger logger = LoggerFactory.getLogger(TransportRequestHandler.class);

  /** The Netty channel that this handler is associated with.
   * 这个处理程序关联的Netty通道*/
  private final Channel channel;

  /** Client on the same channel allowing us to talk back to the requester.
   * 客户端在同一个通道上,让我们可以回复请求者*/
  private final TransportClient reverseClient;

  /** Handles all RPC messages.
   * 处理所有RPC消息*/
  private final RpcHandler rpcHandler;

  /** Returns each chunk part of a stream.
   * 返回流的每个块部分*/
  private final StreamManager streamManager;

  public TransportRequestHandler(
      Channel channel,
      TransportClient reverseClient,
      RpcHandler rpcHandler) {
    this.channel = channel;
    this.reverseClient = reverseClient;
    this.rpcHandler = rpcHandler;
    this.streamManager = rpcHandler.getStreamManager();
  }

  @Override
  public void exceptionCaught(Throwable cause) {
  }

  @Override
  public void channelUnregistered() {
    streamManager.connectionTerminated(channel);
    rpcHandler.connectionTerminated(reverseClient);
  }

  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
      processFetchRequest((ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }

  private void processFetchRequest(final ChunkFetchRequest req) {
    final String client = NettyUtils.getRemoteAddress(channel);

    logger.trace("Received req from {} to fetch block {}", client, req.streamChunkId);

    ManagedBuffer buf;
    try {
      streamManager.registerChannel(channel, req.streamChunkId.streamId);
      buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
    } catch (Exception e) {
      logger.error(String.format(
        "Error opening block %s for request from %s", req.streamChunkId, client), e);
      respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
      return;
    }

    respond(new ChunkFetchSuccess(req.streamChunkId, buf));
  }

  private void processRpcRequest(final RpcRequest req) {
    try {
      rpcHandler.receive(reverseClient, req.message, new RpcResponseCallback() {
        @Override
        public void onSuccess(byte[] response) {
          respond(new RpcResponse(req.requestId, response));
        }

        @Override
        public void onFailure(Throwable e) {
          respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while invoking RpcHandler#receive() on RPC id " + req.requestId, e);
      respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
    }
  }

  /**
   * Responds to a single message with some Encodable object. If a failure occurs while sending,
   * 使用一些可编码对象响应单个消息,发送时发生故障
   * it will be logged and the channel closed.
   * 它将被记录并且通道关闭
   */
  private void respond(final Encodable result) {
    final String remoteAddress = channel.remoteAddress().toString();
    channel.writeAndFlush(result).addListener(
      new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {
            logger.trace(String.format("Sent result %s to client %s", result, remoteAddress));
          } else {
            logger.error(String.format("Error sending result %s to %s; closing connection",
              result, remoteAddress), future.cause());
            channel.close();
          }
        }
      }
    );
  }
}
