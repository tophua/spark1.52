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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters._

import org.apache.hadoop.conf
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{MessageType, MessageTypeParser}
/**
 * Parquet是一种面向列存存储的文件格式
 * 直接写入Parquet
 */
private[sql] object DirectParquetWriter {
  type RecordBuilder = RecordConsumer => Unit

  /**
   * A testing Parquet [[WriteSupport]] implementation used to write manually constructed Parquet
   * records with arbitrary structures.
   * 测试Parquet[writesupport]用来手动写入Parquet记录任意结构
   */
  private class DirectWriteSupport(schema: MessageType, metadata: Map[String, String])
    extends WriteSupport[RecordBuilder] {

    private var recordConsumer: RecordConsumer = _
    //初始化
    override def init(configuration: conf.Configuration): WriteContext = {
      new WriteContext(schema, metadata.asJava)
    }
    //写操作
    override def write(buildRecord: RecordBuilder): Unit = {
      recordConsumer.startMessage()
      buildRecord(recordConsumer)
      recordConsumer.endMessage()
    }
    //准备写
    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
      this.recordConsumer = recordConsumer
    }
  }
  //直接写入
  def writeDirect
      (path: String, schema: String, metadata: Map[String, String] = Map.empty)
      (f: ParquetWriter[RecordBuilder] => Unit): Unit = {
    //println("==1111==")
    val messageType = MessageTypeParser.parseMessageType(schema)
    val writeSupport = new DirectWriteSupport(messageType, metadata)
    // println("==2222==")
    val parquetWriter = new ParquetWriter[RecordBuilder](new Path(path), writeSupport)
     // println("==3333==")
    try f(parquetWriter) finally parquetWriter.close()
  }
  //消息
  def message(writer: ParquetWriter[RecordBuilder])(builder: RecordBuilder): Unit = {
    writer.write(builder)
  }
  //分组
  def group(consumer: RecordConsumer)(f: => Unit): Unit = {
    consumer.startGroup()
    f
    consumer.endGroup()
  }
  //字段
  def field(consumer: RecordConsumer, name: String, index: Int = 0)(f: => Unit): Unit = {
    consumer.startField(name, index)
    f
    consumer.endField(name, index)
  }
}
