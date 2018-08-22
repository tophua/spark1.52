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

package org.apache.spark.network.yarn;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.sasl.SaslServerBootstrap;
import org.apache.spark.network.sasl.ShuffleSecretManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.shuffle.ExternalShuffleBlockHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.yarn.util.HadoopConfigProvider;

/**
 * An external shuffle service used by Spark on Yarn.
 *
 * This is intended to be a long-running auxiliary service that runs in the NodeManager process.
 * A Spark application may connect to this service by setting `spark.shuffle.service.enabled`.
 * The application also automatically derives the service port through `spark.shuffle.service.port`
 * specified in the Yarn configuration. This is so that both the clients and the server agree on
 * the same port to communicate on.
 *
 * The service also optionally supports authentication. This ensures that executors from one
 * application cannot read the shuffle files written by those from another. This feature can be
 * enabled by setting `spark.authenticate` in the Yarn configuration before starting the NM.
 * Note that the Spark application must also set `spark.authenticate` manually and, unlike in
 * the case of the service port, will not inherit this setting from the Yarn configuration. This
 * is because an application running on the same Yarn cluster may choose to not use the external
 * shuffle service, in which case its setting of `spark.authenticate` should be independent of
 * the service's.
 */
public class YarnShuffleService extends AuxiliaryService {
  private final Logger logger = LoggerFactory.getLogger(YarnShuffleService.class);

  // Port on which the shuffle server listens for fetch requests
    //随机播放服务器侦听提取请求的端口
  private static final String SPARK_SHUFFLE_SERVICE_PORT_KEY = "spark.shuffle.service.port";
  private static final int DEFAULT_SPARK_SHUFFLE_SERVICE_PORT = 7337;

  // Whether the shuffle server should authenticate fetch requests
    //随机播放服务器是否应验证抓取请求
  private static final String SPARK_AUTHENTICATE_KEY = "spark.authenticate";
  private static final boolean DEFAULT_SPARK_AUTHENTICATE = false;

  // An entity that manages the shuffle secret per application
  // This is used only if authentication is enabled
    //管理每个应用程序的随机密钥的实体
  //仅在启用身份验证时使用
  private ShuffleSecretManager secretManager;

  // The actual server that serves shuffle files
    //提供随机播放文件的实际服务器
  private TransportServer shuffleServer = null;

  // Handles registering executors and opening shuffle blocks
    //处理注册执行者并打开洗牌
  private ExternalShuffleBlockHandler blockHandler;

  public YarnShuffleService() {
    super("spark_shuffle");
    logger.info("Initializing YARN shuffle service for Spark");
  }

  /**
   * Return whether authentication is enabled as specified by the configuration.
   * 返回是否按配置指定启用身份验证
   * If so, fetch requests will fail unless the appropriate authentication secret
   * for the application is provided.
   * 如果是这样,除非提供了应用程序的相应身份验证机密,否则获取请求将失败
   */
  private boolean isAuthenticationEnabled() {
    return secretManager != null;
  }

  /**
   * Start the shuffle server with the given configuration.
   * 使用给定的配置启动洗牌服务器
   */
  @Override
  protected void serviceInit(Configuration conf) {
    TransportConf transportConf = new TransportConf(new HadoopConfigProvider(conf));
    // If authentication is enabled, set up the shuffle server to use a
    // special RPC handler that filters out unauthenticated fetch requests
    boolean authEnabled = conf.getBoolean(SPARK_AUTHENTICATE_KEY, DEFAULT_SPARK_AUTHENTICATE);
    blockHandler = new ExternalShuffleBlockHandler(transportConf);

    List<TransportServerBootstrap> bootstraps = Lists.newArrayList();
    if (authEnabled) {
      secretManager = new ShuffleSecretManager();
      bootstraps.add(new SaslServerBootstrap(transportConf, secretManager));
    }

    int port = conf.getInt(
      SPARK_SHUFFLE_SERVICE_PORT_KEY, DEFAULT_SPARK_SHUFFLE_SERVICE_PORT);
    TransportContext transportContext = new TransportContext(transportConf, blockHandler);
    shuffleServer = transportContext.createServer(port, bootstraps);
    String authEnabledString = authEnabled ? "enabled" : "not enabled";
    logger.info("Started YARN shuffle service for Spark on port {}. " +
      "Authentication is {}.", port, authEnabledString);
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {
    String appId = context.getApplicationId().toString();
    try {
      ByteBuffer shuffleSecret = context.getApplicationDataForService();
      logger.info("Initializing application {}", appId);
      if (isAuthenticationEnabled()) {
        secretManager.registerApp(appId, shuffleSecret);
      }
    } catch (Exception e) {
      logger.error("Exception when initializing application {}", appId, e);
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    String appId = context.getApplicationId().toString();
    try {
      logger.info("Stopping application {}", appId);
      if (isAuthenticationEnabled()) {
        secretManager.unregisterApp(appId);
      }
      blockHandler.applicationRemoved(appId, false /* clean up local dirs */);
    } catch (Exception e) {
      logger.error("Exception when stopping application {}", appId, e);
    }
  }

  @Override
  public void initializeContainer(ContainerInitializationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.info("Initializing container {}", containerId);
  }

  @Override
  public void stopContainer(ContainerTerminationContext context) {
    ContainerId containerId = context.getContainerId();
    logger.info("Stopping container {}", containerId);
  }

  /**
   * Close the shuffle server to clean up any associated state.
   * 关闭随机服务器以清除任何关联的状态
   */
  @Override
  protected void serviceStop() {
    try {
      if (shuffleServer != null) {
        shuffleServer.close();
      }
    } catch (Exception e) {
      logger.error("Exception when stopping service", e);
    }
  }

  // Not currently used
  @Override
  public ByteBuffer getMetaData() {
      //ByteBuffer.allocate在能够读和写之前,必须有一个缓冲区,用静态方法 allocate() 来分配缓冲区
    return ByteBuffer.allocate(0);
  }

}
