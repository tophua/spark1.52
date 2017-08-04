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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.util.NettyUtils;

/**
 * The single Transport-level Channel handler which is used for delegating requests to the
 * {@link TransportRequestHandler} and responses to the {@link TransportResponseHandler}.
 *
 * 用于将请求委托给的单个传输级信道处理程序{@link TransportRequestHandler}和对{@link TransportResponseHandler}的响应。
 *
 * All channels created in the transport layer are bidirectional. When the Client initiates a Netty
 * Channel with a RequestMessage (which gets handled by the Server's RequestHandler), the Server
 * will produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server
 * also gets a handle on the same Channel, so it may then begin to send RequestMessages to the
 * Client.
 *
 * 在传输层中创建的所有通道都是双向的,当客户启动Netty具有RequestMessage（由服务器的RequestHandler处理）的通道,
 * 服务器将产生一个ResponseMessage(由客户端的ResponseHandler处理)。 但是,服务器也可以在同一个频道上获得一个句柄,
 * 所以可以开始向其发送RequestMessages客户。
 *
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 *
 * 这意味着客户端还需要一个RequestHandler，并且服务器需要一个ResponseHandler用于客户对服务器请求的响应
 *
 * This class also handles timeouts from a {@link io.netty.handler.timeout.IdleStateHandler}.
 * We consider a connection timed out if there are outstanding fetch or RPC requests but no traffic
 * on the channel for at least `requestTimeoutMs`. Note that this is duplex traffic; we will not
 * timeout if the client is continuously sending but getting no responses, for simplicity.
 */
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
  private final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

  private final TransportClient client;
  private final TransportResponseHandler responseHandler;
  private final TransportRequestHandler requestHandler;
  private final long requestTimeoutNs;

  public TransportChannelHandler(
      TransportClient client,
      TransportResponseHandler responseHandler,
      TransportRequestHandler requestHandler,
      long requestTimeoutMs) {
    this.client = client;
    this.responseHandler = responseHandler;
    this.requestHandler = requestHandler;
    this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
  }

  public TransportClient getClient() {
    return client;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Exception in connection from " + NettyUtils.getRemoteAddress(ctx.channel()),
      cause);
    requestHandler.exceptionCaught(cause);
    responseHandler.exceptionCaught(cause);
    ctx.close();
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    try {
      requestHandler.channelUnregistered();
    } catch (RuntimeException e) {
      logger.error("Exception from request handler while unregistering channel", e);
    }
    try {
      responseHandler.channelUnregistered();
    } catch (RuntimeException e) {
      logger.error("Exception from response handler while unregistering channel", e);
    }
    super.channelUnregistered(ctx);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, Message request) {
    if (request instanceof RequestMessage) {
      requestHandler.handle((RequestMessage) request);
    } else {
      responseHandler.handle((ResponseMessage) request);
    }
  }

  /** Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}.
   * 根据{@link io.netty.handler.timeout.IdleStateHandler}的事件触发。*/
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      // See class comment for timeout semantics. In addition to ensuring we only timeout while
      // there are outstanding requests, we also do a secondary consistency check to ensure
      // there's no race between the idle timeout and incrementing the numOutstandingRequests.
        //查看超时语义的类注释。 除了确保我们只有超时的同时有未完成的请求,我们也做了二次一致性检查,以确保
        //空闲超时和递增numOutstandingRequests之间没有竞争
      boolean hasInFlightRequests = responseHandler.numOutstandingRequests() > 0;
      boolean isActuallyOverdue =
        System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
      if (e.state() == IdleState.ALL_IDLE && hasInFlightRequests && isActuallyOverdue) {
        String address = NettyUtils.getRemoteAddress(ctx.channel());
        logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
          "requests. Assuming connection is dead; please adjust spark.network.timeout if this " +
          "is wrong.", address, requestTimeoutNs / 1000 / 1000);
        ctx.close();
      }
    }
  }
}
