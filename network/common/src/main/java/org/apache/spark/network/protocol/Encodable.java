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

import io.netty.buffer.ByteBuf;

/**
 * Interface for an object which can be encoded into a ByteBuf. Multiple Encodable objects are
 * stored in a single, pre-allocated ByteBuf, so Encodables must also provide their length.
 *
 * 可以编码成ByteBuf的对象的接口,多个可编码对象存储在单个预分配的ByteBuf中,因此Encodables还必须提供其长度
 *
 * Encodable objects should provide a static "decode(ByteBuf)" method which is invoked by
 * {@link MessageDecoder}. During decoding, if the object uses the ByteBuf as its data (rather than
 * just copying data from it), then you must retain() the ByteBuf.
 *
 * 可编码对象应提供一个由{@link MessageDecoder}调用的静态“decode（ByteBuf）”方法。
 * 在解码期间，如果对象使用ByteBuf作为其数据（而不是仅从其中复制数据），则必须保留（）ByteBuf。
 *
 * Additionally, when adding a new Encodable Message, add it to {@link Message.Type}.
 */
public interface Encodable {
  /** Number of bytes of the encoded form of this object.
   * 该对象的编码形式的字节数 */
  int encodedLength();

  /**
   * Serializes this object by writing into the given ByteBuf.
   * 通过写入给定的ByteBuf来序列化此对象
   * This method must write exactly encodedLength() bytes.
   * 这个方法必须写正确的encodedLength()字节
   */
  void encode(ByteBuf buf);
}
