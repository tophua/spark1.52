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

package org.apache.spark.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.generic.GenericData.Record

import org.apache.spark.{SparkFunSuite, SharedSparkContext}

class GenericAvroSerializerSuite extends SparkFunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val schema : Schema = SchemaBuilder
    .record("testRecord").fields()
    .requiredString("data")
    .endRecord()
  val record = new Record(schema)
  record.put("data", "test data")

  test("schema compression and decompression") {//模式压缩与解压缩
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    assert(schema === genericSer.decompress(ByteBuffer.wrap(genericSer.compress(schema))))
  }

  test("record serialization and deserialization") {//记录序列化和反序列化
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSer.serializeDatum(record, output)
    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSer.deserializeDatum(input) === record)
  }
  //使用模式指纹以减少信息大小
  test("uses schema fingerprint to decrease message size") {
    val genericSerFull = new GenericAvroSerializer(conf.getAvroSchema)

    val output = new Output(new ByteArrayOutputStream())

    val beginningNormalPosition = output.total()
    genericSerFull.serializeDatum(record, output)
    output.flush()
    val normalLength = output.total - beginningNormalPosition

    conf.registerAvroSchemas(schema)
    val genericSerFinger = new GenericAvroSerializer(conf.getAvroSchema)
    val beginningFingerprintPosition = output.total()
    genericSerFinger.serializeDatum(record, output)
    val fingerprintLength = output.total - beginningFingerprintPosition

    assert(fingerprintLength < normalLength)
  }

  test("caches previously seen schemas") {//缓存之前模式
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    val compressedSchema = genericSer.compress(schema)
    val decompressedScheam = genericSer.decompress(ByteBuffer.wrap(compressedSchema))

    assert(compressedSchema.eq(genericSer.compress(schema)))
    assert(decompressedScheam.eq(genericSer.decompress(ByteBuffer.wrap(compressedSchema))))
  }
}
