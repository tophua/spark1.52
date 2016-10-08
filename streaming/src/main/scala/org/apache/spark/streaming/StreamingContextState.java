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

package org.apache.spark.streaming;

import org.apache.spark.annotation.DeveloperApi;

/**
 * :: DeveloperApi ::
 *
 * Represents the state of a StreamingContext.
 */
@DeveloperApi
public enum StreamingContextState {
  /**
   * The context has been created, but not been started yet.
   * Input DStreams, transformations and output operations can be created on the context.
   * 创建应用上下文件,但尚未开始
   */
  INITIALIZED,

  /**
   * The context has been started, and been not stopped.
   * 上下文已开始,并没有停止.
   * Input DStreams, transformations and output operations cannot be created on the context.
   */
  ACTIVE,

  /**
   * The context has been stopped and cannot be used any more.
   * 上下文已被停止,不能再使用了
   */
  STOPPED
}
