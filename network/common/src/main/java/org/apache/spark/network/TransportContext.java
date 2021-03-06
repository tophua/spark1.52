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

package org.apache.spark.network;

import java.util.List;

import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 * 构造传输上下文TransportContext
 * 即可以创建Netty服务,也可以创建Netty访问客户端
 */
public class TransportContext {
  private final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private final TransportConf conf; //主要控制Netty框架提供的Shuffle的I/O交互的客户端线程数量
  private final RpcHandler rpcHandler;//负责shuffle的I/O服务端在接收客户端的RPC请求后,提供打开Block
  									  //或者上传Block的RPC处理,此处即NettyBlockRPCServer
  //在Shuffle的I/O服务端在对客户端传来的ByteBuf进行解析,防止丢包和解析错误
  private final MessageEncoder encoder;
  //在Shuffle的I/O客户端对消息内容进行编码,防止丢包和解析错误
  private final MessageDecoder decoder;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    //在Shuffle的I/O客户端对消息内容进行编码,防止丢包和解析错误
    this.encoder = new MessageEncoder();    
    //在Shuffle的I/O服务端对客户端传来的ByteBuffer进行解析,防止丢包和解析错误
    this.decoder = new MessageDecoder();
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   * 在返回新客户端之前初始化运行给定TransportClientBootstraps的ClientFactory,
   * Bootstraps将同步执行,并且必须成功运行才能创建客户端。
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(Lists.<TransportClientBootstrap>newArrayList());
  }

  /** Create a server which will attempt to bind to a specific port.
   * 创建一个尝试绑定到特定端口的服务器*/
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port.
   * 创建一个新的服务器，绑定到任何可用的临时端口*/
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, Lists.<TransportServerBootstrap>newArrayList());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * 初始化客户端或服务器Netty Channel Pipeline,用于对消息进行编码/解码,
   * 并具有{@link org.apache.spark.network.server.TransportChannelHandler}来处理请求或响应消
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", encoder)
        .addLast("frameDecoder", NettyUtils.createFrameDecoder())
        .addLast("decoder", decoder)
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
              //注意：块根据请求的顺序被保证被返回,但是这样做将需要更多的逻辑来保证这不是同一事件循环的一部分。
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   *
   * 创建用于处理RequestMessages和ResponseMessages的服务器端和客户端处理程序,
   * 尽管某些属性（如remoteAddress（））可能尚未可用,但预期该通道已成功创建,
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler);
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs());
  }

  public TransportConf getConf() { return conf; }
}
