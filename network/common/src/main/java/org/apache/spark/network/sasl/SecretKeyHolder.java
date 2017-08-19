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

/**
 * Interface for getting a secret key associated with some application.
 * 用于获取与某些应用程序相关联的密钥的接口
 */
public interface SecretKeyHolder {
  /**
   * Gets an appropriate SASL User for the given appId.
   * 为给定的appId获取适当的SASL用户
   * @throws IllegalArgumentException if the given appId is not associated with a SASL user.
   * 如果给定的appId不与SASL用户相关联
   */
  String getSaslUser(String appId);

  /**
   * Gets an appropriate SASL secret key for the given appId.
   * 为给定的appId获取适当的SASL密钥
   * @throws IllegalArgumentException if the given appId is not associated with a SASL secret key.
   * 如果给定的appId不与SASL密钥相关联
   */
  String getSecretKey(String appId);
}
