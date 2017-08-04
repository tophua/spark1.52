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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encoder used by the server side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 * 服务器端使用的编码器对服务器到客户端的响应进行编码,该编码器是无状态的,因此可以安全地由多个线程共享
 */
@ChannelHandler.Sharable
public final class MessageEncoder extends MessageToMessageEncoder<Message> {

  private final Logger logger = LoggerFactory.getLogger(MessageEncoder.class);

  /***
   * Encodes a Message by invoking its encode() method. For non-data messages, we will add one
   * ByteBuf to 'out' containing the total frame length, the message type, and the message itself.
   * In the case of a ChunkFetchSuccess, we will also add the ManagedBuffer corresponding to the
   * data to 'out', in order to enable zero-copy transfer.
   * 通过调用其encode()方法来对消息进行编码,对于非数据消息,我们将添加一个ByteBuf到'out'包含总帧长度,消息类型和消息本身。
   *在ChunkFetchSuccess的情况下，我们还会添加对应的ManagedBuffer数据到'出',以便启用零拷贝传输。
   */
  @Override
  public void encode(ChannelHandlerContext ctx, Message in, List<Object> out) {
    Object body = null;
    long bodyLength = 0;

    // Only ChunkFetchSuccesses have data besides the header.
      //只有ChunkFetchSuccess除头之外还有数据
    // The body is used in order to enable zero-copy transfer for the payload.
      // body被用于为有效载荷启用零拷贝传输。
    if (in instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess resp = (ChunkFetchSuccess) in;
      try {
        bodyLength = resp.buffer.size();
        body = resp.buffer.convertToNetty();
      } catch (Exception e) {
        // Re-encode this message as BlockFetchFailure.
          //将此消息重新编码为BlockFetchFailure
        logger.error(String.format("Error opening block %s for client %s",
          resp.streamChunkId, ctx.channel().remoteAddress()), e);
        encode(ctx, new ChunkFetchFailure(resp.streamChunkId, e.getMessage()), out);
        return;
      }
    }

    Message.Type msgType = in.type();
    // All messages have the frame length, message type, and message itself.
      //所有消息都具有帧长度，消息类型和消息本身。
    int headerLength = 8 + msgType.encodedLength() + in.encodedLength();
    long frameLength = headerLength + bodyLength;
    ByteBuf header = ctx.alloc().heapBuffer(headerLength);
    header.writeLong(frameLength);
    msgType.encode(header);
    in.encode(header);
    assert header.writableBytes() == 0;

    if (body != null && bodyLength > 0) {
      out.add(new MessageWithHeader(header, body, bodyLength));
    } else {
      out.add(header);
    }
  }

}
