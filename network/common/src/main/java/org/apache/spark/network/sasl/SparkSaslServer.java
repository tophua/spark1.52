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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.base64.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SASL Server for Spark which simply keeps track of the state of a single SASL session, from the
 * initial state to the "authenticated" state. (It is not a server in the sense of accepting
 * connections on some socket.)
 * 用于Spark的SASL服务器简单地跟踪单个SASL会话的状态，从初始状态到“已认证”状态。
 * (它不是在某个套接字上接受连接的服务器。)
 */
public class SparkSaslServer implements SaslEncryptionBackend {
  private final Logger logger = LoggerFactory.getLogger(SparkSaslServer.class);

  /**
   * This is passed as the server name when creating the sasl client/server.
   * 在创建sasl客户端/服务器时，将作为服务器名称传递
   * This could be changed to be configurable in the future.
   * 这可以在将来更改为可配置
   */
  static final String DEFAULT_REALM = "default";

  /**
   * The authentication mechanism used here is DIGEST-MD5. This could be changed to be
   * configurable in the future.
   * 这里使用的认证机制是DIGEST-MD5,这可以在将来更改为可配置
   */
  static final String DIGEST = "DIGEST-MD5";

  /**
   * Quality of protection value that includes encryption.
   * 保护质量包括加密值
   */
  static final String QOP_AUTH_CONF = "auth-conf";

  /**
   * Quality of protection value that does not include encryption.
   * 不包括加密的保护质量值
   */
  static final String QOP_AUTH = "auth";

  /** Identifier for a certain secret key within the secretKeyHolder.
   * secretKeyHolder内的某个秘密密钥的标识符*/
  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;
  private SaslServer saslServer;

  public SparkSaslServer(
      String secretKeyId,
      SecretKeyHolder secretKeyHolder,
      boolean alwaysEncrypt) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;

    // Sasl.QOP is a comma-separated list of supported values. The value that allows encryption
    // is listed first since it's preferred over the non-encrypted one (if the client also
    // lists both in the request).
      //Sasl.QOP是以逗号分隔的支持值列表,首先列出允许加密的值,因为它优先于未加密的值（如果客户端也在请求中同时列出）。
    String qop = alwaysEncrypt ? QOP_AUTH_CONF : String.format("%s,%s", QOP_AUTH_CONF, QOP_AUTH);
    Map<String, String> saslProps = ImmutableMap.<String, String>builder()
      .put(Sasl.SERVER_AUTH, "true")
      .put(Sasl.QOP, qop)
      .build();
    try {
      this.saslServer = Sasl.createSaslServer(DIGEST, null, DEFAULT_REALM, saslProps,
        new DigestCallbackHandler());
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Determines whether the authentication exchange has completed successfully.
   * 确定认证交换是否已成功完成
   */
  public synchronized boolean isComplete() {
    return saslServer != null && saslServer.isComplete();
  }

  /** Returns the value of a negotiated property.
   * 返回协商属性的值 */
  public Object getNegotiatedProperty(String name) {
    return saslServer.getNegotiatedProperty(name);
  }

  /**
   * Used to respond to server SASL tokens.
   * 用于响应服务器SASL令牌
   * @param token Server's SASL token
   * @return response to send back to the server.
   */
  public synchronized byte[] response(byte[] token) {
    try {
      return saslServer != null ? saslServer.evaluateResponse(token) : new byte[0];
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the
   * SaslServer might be using.
   * 处理SaslServer可能使用的任何系统资源或安全敏感信息
   */
  @Override
  public synchronized void dispose() {
    if (saslServer != null) {
      try {
        saslServer.dispose();
      } catch (SaslException e) {
        // ignore
      } finally {
        saslServer = null;
      }
    }
  }

  @Override
  public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
    return saslServer.wrap(data, offset, len);
  }

  @Override
  public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
    return saslServer.unwrap(data, offset, len);
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler for SASL DIGEST-MD5 mechanism.
   * 实现用于SASL DIGEST-MD5机制的javax.security.auth.callback.CallbackHandler
   */
  private class DigestCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          logger.trace("SASL server callback: setting username");
          NameCallback nc = (NameCallback) callback;
          nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
        } else if (callback instanceof PasswordCallback) {
          logger.trace("SASL server callback: setting password");
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
        } else if (callback instanceof RealmCallback) {
          logger.trace("SASL server callback: setting realm");
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else if (callback instanceof AuthorizeCallback) {
          AuthorizeCallback ac = (AuthorizeCallback) callback;
          String authId = ac.getAuthenticationID();
          String authzId = ac.getAuthorizationID();
          ac.setAuthorized(authId.equals(authzId));
          if (ac.isAuthorized()) {
            ac.setAuthorizedID(authzId);
          }
          logger.debug("SASL Authorization complete, authorized set to {}", ac.isAuthorized());
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
    }
  }

  /* Encode a byte[] identifier as a Base64-encoded string.
  * 将byte []标识符编码为Base64编码的字符串*/
  public static String encodeIdentifier(String identifier) {
    Preconditions.checkNotNull(identifier, "User cannot be null if SASL is enabled");
    return Base64.encode(Unpooled.wrappedBuffer(identifier.getBytes(Charsets.UTF_8)))
      .toString(Charsets.UTF_8);
  }

  /** Encode a password as a base64-encoded char[] array.
   * 将密码编码为base64编码的char []数组*/
  public static char[] encodePassword(String password) {
    Preconditions.checkNotNull(password, "Password cannot be null if SASL is enabled");
    return Base64.encode(Unpooled.wrappedBuffer(password.getBytes(Charsets.UTF_8)))
      .toString(Charsets.UTF_8).toCharArray();
  }
}
