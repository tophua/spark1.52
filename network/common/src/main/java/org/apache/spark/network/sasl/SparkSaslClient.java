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

import java.io.IOException;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.network.sasl.SparkSaslServer.*;

/**
 * A SASL Client for Spark which simply keeps track of the state of a single SASL session, from the
 * initial state to the "authenticated" state. This client initializes the protocol via a
 * firstToken, which is then followed by a set of challenges and responses.
 *
 * 一个用于Spark的SASL客户端,它简单地跟踪单个SASL会话的状态,从初始状态到“已认证”状态。
 * 该客户端通过firstToken初始化协议，然后是一组挑战和响应。
 */
public class SparkSaslClient implements SaslEncryptionBackend {
  private final Logger logger = LoggerFactory.getLogger(SparkSaslClient.class);

  private final String secretKeyId;
  private final SecretKeyHolder secretKeyHolder;
  private final String expectedQop;
  private SaslClient saslClient;

  public SparkSaslClient(String secretKeyId, SecretKeyHolder secretKeyHolder, boolean encrypt) {
    this.secretKeyId = secretKeyId;
    this.secretKeyHolder = secretKeyHolder;
    this.expectedQop = encrypt ? QOP_AUTH_CONF : QOP_AUTH;

    Map<String, String> saslProps = ImmutableMap.<String, String>builder()
      .put(Sasl.QOP, expectedQop)
      .build();
    try {
      this.saslClient = Sasl.createSaslClient(new String[] { DIGEST }, null, null, DEFAULT_REALM,
        saslProps, new ClientCallbackHandler());
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /** Used to initiate SASL handshake with server.
   * 用于启动与服务器的SASL握手*/
  public synchronized byte[] firstToken() {
    if (saslClient != null && saslClient.hasInitialResponse()) {
      try {
        return saslClient.evaluateChallenge(new byte[0]);
      } catch (SaslException e) {
        throw Throwables.propagate(e);
      }
    } else {
      return new byte[0];
    }
  }

  /** Determines whether the authentication exchange has completed.
   * 确定认证交换是否已完成*/
  public synchronized boolean isComplete() {
    return saslClient != null && saslClient.isComplete();
  }

  /** Returns the value of a negotiated property.
   * 返回协商议属性的值*/
  public Object getNegotiatedProperty(String name) {
    return saslClient.getNegotiatedProperty(name);
  }

  /**
   * Respond to server's SASL token.
   * 响应服务器的SASL令牌
   * @param token contains server's SASL token
   * @return client's response SASL token
   */
  public synchronized byte[] response(byte[] token) {
    try {
      return saslClient != null ? saslClient.evaluateChallenge(token) : new byte[0];
    } catch (SaslException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Disposes of any system resources or security-sensitive information the
   * SaslClient might be using.
   * 处理SaslClient可能使用的任何系统资源或安全敏感信息
   */
  @Override
  public synchronized void dispose() {
    if (saslClient != null) {
      try {
        saslClient.dispose();
      } catch (SaslException e) {
        // ignore
      } finally {
        saslClient = null;
      }
    }
  }

  /**
   * Implementation of javax.security.auth.callback.CallbackHandler
   * that works with share secrets.
   * 实现与共享密钥一起使用的javax.security.auth.callback.CallbackHandler
   */
  private class ClientCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          logger.trace("SASL client callback: setting username");
          NameCallback nc = (NameCallback) callback;
          nc.setName(encodeIdentifier(secretKeyHolder.getSaslUser(secretKeyId)));
        } else if (callback instanceof PasswordCallback) {
          logger.trace("SASL client callback: setting password");
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(encodePassword(secretKeyHolder.getSecretKey(secretKeyId)));
        } else if (callback instanceof RealmCallback) {
          logger.trace("SASL client callback: setting realm");
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else if (callback instanceof RealmChoiceCallback) {
          // ignore (?)
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
    }
  }

  @Override
  public byte[] wrap(byte[] data, int offset, int len) throws SaslException {
    return saslClient.wrap(data, offset, len);
  }

  @Override
  public byte[] unwrap(byte[] data, int offset, int len) throws SaslException {
    return saslClient.unwrap(data, offset, len);
  }

}
