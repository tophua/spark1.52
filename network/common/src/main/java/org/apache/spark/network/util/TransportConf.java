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

package org.apache.spark.network.util;

import com.google.common.primitives.Ints;

/**
 * A central location that tracks all the settings we expose to users.
 * 主要控制Netty框架提供的shuffle的I/O交互的客户端和服务端线程的数量
 */
public class TransportConf {
  private final ConfigProvider conf;

  public TransportConf(ConfigProvider conf) {
    this.conf = conf;
  }

  /** IO mode: nio or epoll */
  public String ioMode() { return conf.get("spark.shuffle.io.mode", "NIO").toUpperCase(); }

  /** If true, we will prefer allocating off-heap byte buffers within Netty. */
  public boolean preferDirectBufs() {
    return conf.getBoolean("spark.shuffle.io.preferDirectBufs", true);
  }

  /** 
   * Connect timeout in milliseconds. Default 120 secs. 
   * 连接超时毫秒,默认120秒
   * */
  public int connectionTimeoutMs() {
    long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(
      conf.get("spark.network.timeout", "120s"));
    long defaultTimeoutMs = JavaUtils.timeStringAsSec(
      conf.get("spark.shuffle.io.connectionTimeout", defaultNetworkTimeoutS + "s")) * 1000;
    return (int) defaultTimeoutMs;
  }

  /** 
   * Number of concurrent connections between two nodes for fetching data. 
   * 获取数据的两个节点之间的并发连接数,默认为1
   * */
  public int numConnectionsPerPeer() {
    return conf.getInt("spark.shuffle.io.numConnectionsPerPeer", 1);
  }

  /** 
   * Requested maximum length of the queue of incoming connections. Default -1 for no backlog.
   * 请求的传入连接的队列的最大长度。默认- 1,没有限制
   * */
  public int backLog() { return conf.getInt("spark.shuffle.io.backLog", -1); }

  /** 
   * Number of threads used in the server thread pool. Default to 0, which is 2x#cores. 
   * 服务器线程池中使用的线程数,默认0,使用CPU核数
   * */
  public int serverThreads() { return conf.getInt("spark.shuffle.io.serverThreads", 0); }

  /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
  public int clientThreads() { return conf.getInt("spark.shuffle.io.clientThreads", 0); }

  /**
   * Receive buffer size (SO_RCVBUF).
   * 接收缓冲区的大小,最佳的接收缓冲区和发送缓冲区的大小应网络延迟X网络带宽
   * Note: the optimal size for receive buffer and send buffer should be
   *  latency * network_bandwidth.
   *  假设 网络延迟= 1ms毫秒  网络带宽=10G,缓存区大小应该1.25MB
   * Assuming latency = 1ms, network_bandwidth = 10Gbps
   *  buffer size should be ~ 1.25MB
   */
  public int receiveBuf() { return conf.getInt("spark.shuffle.io.receiveBuffer", -1); }

  /** 
   * Send buffer size (SO_SNDBUF). 
   * 发送缓冲区大小
   * */
  public int sendBuf() { return conf.getInt("spark.shuffle.io.sendBuffer", -1); }

  /** 
   * Timeout for a single round trip of SASL token exchange, in milliseconds. 
   * 对于单往返SASL令牌交换超时时间，以毫秒为单位。
   * */
  public int saslRTTimeoutMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get("spark.shuffle.sasl.timeout", "30s")) * 1000;
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request.
   * If set to 0, we will not do any retries.
   * 请求连接最大尝试数,如果0,不做任何重试
   */
  public int maxIORetries() { return conf.getInt("spark.shuffle.io.maxRetries", 3); }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
   * Only relevant if maxIORetries &gt; 0.
   *等待进行再连接时间,时间(以毫秒为单位)
   */
  public int ioRetryWaitTimeMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get("spark.shuffle.io.retryWait", "5s")) * 1000;
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   * 以字节为单位的块大小，用于磁盘读取一个块大小进行内存映射。这可以防止Spark在内存映射时使用很小块
   * 一般情况下，对块进行内存映射的开销接近或低于操作系统的页大小。
   */
  public int memoryMapBytes() {
    return conf.getInt("spark.storage.memoryMapThreshold", 2 * 1024 * 1024);
  }

  /**
   * Whether to initialize shuffle FileDescriptor lazily or not. If true, file descriptors are
   * created only when data is going to be transferred. This can reduce the number of open files.
   */
  public boolean lazyFileDescriptor() {
    return conf.getBoolean("spark.shuffle.io.lazyFD", true);
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   * 绑定到一个端口在放弃之前,最大重试次数时.
   */
  public int portMaxRetries() {
    return conf.getInt("spark.port.maxRetries", 16);
  }

  /**
   * Maximum number of bytes to be encrypted at a time when SASL encryption is enabled.
   */
  public int maxSaslEncryptedBlockSize() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.network.sasl.maxEncryptedBlockSize", "64k")));
  }

  /**
   * Whether the server should enforce encryption on SASL-authenticated connections.
   */
  public boolean saslServerAlwaysEncrypt() {
    return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt", false);
  }

}
