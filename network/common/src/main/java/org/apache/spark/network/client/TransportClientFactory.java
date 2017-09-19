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

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 * 通过使用createClient创建{@link TransportClient}的工厂
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 * 工厂将维护与其他主机的连接池,并应为相同的远程主机返回相同的TransportClient,它还为所有TransportClients共享一个工作线程池
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 * RPC客户端工厂TransportClientFactory,用于向Netty服务端发送RPC请求,TransportContext的CreateClientFactory
 * 方法用于创建TransportClientFactory
 */
public class TransportClientFactory implements Closeable {

  /** 
   * A simple data structure to track the pool of clients between two peer nodes.
   * 一个简单的数据结构来跟踪两个对等节点之间的客户端 
   * */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    public ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final List<TransportClientBootstrap> clientBootstraps;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /** 
   * Random number generator for picking connections between peers. 
   * 随机产生开始连接
   * */
  private final Random rand;
  private final int numConnectionsPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;

  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    //用于缓存客户端列表
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    //用于缓存客户端连接
    this.connectionPool = new ConcurrentHashMap<SocketAddress, ClientPool>();
    //节点之间取数据的连接数,可以使用spark.shuffle.io.numConnectionsPerPeer来设置默认1
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    //客户端channel被创建时使用的类,可以使用属性spark.shuffle.io.mode来配置
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    // TODO: Make thread pool name configurable.
    //根据Netty的规范,客户端只有work组,所以此处创建workerGroup
    this.workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "shuffle-client");
    //汇集ByteBuf,但对本地线程缓存禁用的分配器
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   * 创建一个{@link TransportClient}连接到给定的远程主机/端口
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   *
   * 我们维护一个客户端数组（大小由spark.shuffle.io.numConnectionsPerPeer确定）,
   * 并随机选择一个使用,如果以前在随机选择的地点没有创建客户端,则此功能将创建一个新客户端并将其放置在该位置。
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   *
   * 在创建新的TransportClient之前，我们将执行在此工厂注册的所有{@link TransportClientBootstrap}
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   * 直到连接成功建立并完全自引导为止
   *
   * Concurrency: This method is safe to call from multiple threads.
   * 并发性：这种方法可以安全地从多个线程调用
   */
  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    // Get connection from the connection pool first.
      //首先从连接池获取连接
    // If it is not found or not active, create a new one.
      //如果找不到或不活动,请创建一个新的
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
      //如果还没有创建ClientPool
    ClientPool clientPool = connectionPool.get(address);
    if (clientPool == null) {
      connectionPool.putIfAbsent(address, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(address);
    }

    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    if (cachedClient != null && cachedClient.isActive()) {
      logger.trace("Returning cached connection to {}: {}", address, cachedClient);
      return cachedClient;
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
      //如果我们到达这里,我们没有打开现有的连接,我们来创建一个新的,个线程可能会在这里竞争创建新的连接,只保留其中一个活动。
    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          logger.trace("Returning cached connection to {}: {}", address, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", address);
        }
      }
      clientPool.clients[clientIndex] = createClient(address);
      return clientPool.clients[clientIndex];
    }
  }

  /** Create a completely new {@link TransportClient} to the remote address.
   * 创建一个全新的{@link TransportClient}到远程地址*/
  private TransportClient createClient(InetSocketAddress address) throws IOException {
    logger.debug("Creating new connection to " + address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait
            // 禁用Nagle的算法，因为我们不希望数据包等待
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    final AtomicReference<TransportClient> clientRef = new AtomicReference<TransportClient>();
    final AtomicReference<Channel> channelRef = new AtomicReference<Channel>();

    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
      //连接到远程服务器
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
      throw new IOException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
      //在将客户端标记为成功之前,同步执行任何客户端引导
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      //捕获非RuntimeExceptions也可以在Scala中写入引导
        long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.debug("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }

  /** 
   * Close all connections in the connection pool, and shutdown the worker thread pool. 
   * 关闭连接池中的所有连接,并关闭Woker线程池
   * */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
      //通过所有的客户端,如果他们是活跃的关闭他们
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }
}
