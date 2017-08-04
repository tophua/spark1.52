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

package org.apache.spark.network.sasl;

import javax.security.sasl.Sasl;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.TransportConf;

/**
 * RPC Handler which performs SASL authentication before delegating to a child RPC handler.
 * The delegate will only receive messages if the given connection has been successfully
 * authenticated. A connection may be authenticated at most once.
 *
 * rpc处理器执行SASL认证授权前对一个孩子的RPC处理程序,代表只会在给定的连接已成功验证接收消息,一个连接可以认证最多一次
 *
 * Note that the authentication process consists of multiple challenge-response pairs, each of
 * which are individual RPCs.
 * 请注意,认证过程由多个挑战 - 响应对组成,每个都是单个RPC,
 */
class SaslRpcHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);

  /** Transport configuration. 传输配置*/
  private final TransportConf conf;

  /** The client channel. 客户端channel*/
  private final Channel channel;

  /** RpcHandler we will delegate to for authenticated connections.
   * 我们将委托RpcHandler进行认证连接*/
  private final RpcHandler delegate;

  /** Class which provides secret keys which are shared by server and client on a per-app basis.
   * 提供按照应用程序的服务器和客户端共享的秘密密钥的类*/
  private final SecretKeyHolder secretKeyHolder;

  private SparkSaslServer saslServer;
  private boolean isComplete;

  SaslRpcHandler(
      TransportConf conf,
      Channel channel,
      RpcHandler delegate,
      SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    this.channel = channel;
    this.delegate = delegate;
    this.secretKeyHolder = secretKeyHolder;
    this.saslServer = null;
    this.isComplete = false;
  }

  @Override
  public void receive(TransportClient client, byte[] message, RpcResponseCallback callback) {
    if (isComplete) {
      // Authentication complete, delegate to base handler.
        //认证完成，委托给基础处理程序
      delegate.receive(client, message, callback);
      return;
    }

    SaslMessage saslMessage = SaslMessage.decode(Unpooled.wrappedBuffer(message));

    if (saslServer == null) {
      // First message in the handshake, setup the necessary state.
        //握手中的第一条消息,设置必要的状态
      saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder,
        conf.saslServerAlwaysEncrypt());
    }

    byte[] response = saslServer.response(saslMessage.payload);
    callback.onSuccess(response);

    // Setup encryption after the SASL response is sent, otherwise the client can't parse the
    // response. It's ok to change the channel pipeline here since we are processing an incoming
    // message, so the pipeline is busy and no new incoming messages will be fed to it before this
    // method returns. This assumes that the code ensures, through other means, that no outbound
    // messages are being written to the channel while negotiation is still going on.
      //发送SASL响应后进行安装加密，否则客户端无法解析响应,因为我们正在处理一个传入的消息，所以改变通道管道就可以了,
      // 所以在这个方法返回之前,流水线很忙,没有新的传入消息被提供给它,这假设代码通过其他方式确保在协商仍然进行时不会将外发消息写入通道。
    if (saslServer.isComplete()) {
      logger.debug("SASL authentication successful for channel {}", client);
      isComplete = true;
      if (SparkSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiatedProperty(Sasl.QOP))) {
        logger.debug("Enabling encryption for channel {}", client);
        SaslEncryption.addToChannel(channel, saslServer, conf.maxSaslEncryptedBlockSize());
        saslServer = null;
      } else {
        saslServer.dispose();
        saslServer = null;
      }
    }
  }

  @Override
  public StreamManager getStreamManager() {
    return delegate.getStreamManager();
  }

  @Override
  public void connectionTerminated(TransportClient client) {
    if (saslServer != null) {
      saslServer.dispose();
    }
  }

}
