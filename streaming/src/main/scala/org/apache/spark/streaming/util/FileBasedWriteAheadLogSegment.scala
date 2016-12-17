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
package org.apache.spark.streaming.util

/** 
 *  Class for representing a segment of data in a write ahead log file 
 *  用于在预写式日志文件中代表数据段的类
 *  path:给定文件
 *  offset:偏移
 *  length:长度
 *  就可以唯一确定一条log
 *  */
private[streaming] case class FileBasedWriteAheadLogSegment(path: String, offset: Long, length: Int)
  extends WriteAheadLogRecordHandle
