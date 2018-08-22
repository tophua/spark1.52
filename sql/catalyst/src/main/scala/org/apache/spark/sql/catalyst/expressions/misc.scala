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

package org.apache.spark.sql.catalyst.expressions

import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util.zip.CRC32

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A function that calculates an MD5 128-bit checksum and returns it as a hex string
 * For input of type [[BinaryType]]
  * 计算MD5 128位校验和并将其作为十六进制字符串返回的函数对于[BinaryType]类型的输入
 */
case class Md5(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.md5Hex(input.asInstanceOf[Array[Byte]]))

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(org.apache.commons.codec.digest.DigestUtils.md5Hex($c))")
  }
}

/**
 * A function that calculates the SHA-2 family of functions (SHA-224, SHA-256, SHA-384, and SHA-512)
 * and returns it as a hex string. The first argument is the string or binary to be hashed. The
 * second argument indicates the desired bit length of the result, which must have a value of 224,
 * 256, 384, 512, or 0 (which is equivalent to 256). SHA-224 is supported starting from Java 8. If
 * asking for an unsupported SHA function, the return value is NULL. If either argument is NULL or
 * the hash length is not one of the permitted values, the return value is NULL.
  * 一种计算SHA-2系列函数（SHA-224，SHA-256，SHA-384和SHA-512）并将其作为十六进制字符串返回的函数,
  * 第一个参数是要进行哈希处理的字符串或二进制文件,第二个参数表示结果的所需位长度,其值必须为224,256,384,512或0(相当于256),
  * 从Java 8开始支持SHA-224,如果要求不支持的SHA函数,则返回值为NULL,如果任一参数为NULL或散列长度不是允许值之一,则返回值为NULL。
 */
case class Sha2(left: Expression, right: Expression)
  extends BinaryExpression with Serializable with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType, IntegerType)

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val bitLength = input2.asInstanceOf[Int]
    val input = input1.asInstanceOf[Array[Byte]]
    bitLength match {
      case 224 =>
        // DigestUtils doesn't support SHA-224 now
        try {
          val md = MessageDigest.getInstance("SHA-224")
          md.update(input)
          UTF8String.fromBytes(md.digest())
        } catch {
          // SHA-224 is not supported on the system, return null
          case noa: NoSuchAlgorithmException => null
        }
      case 256 | 0 =>
        UTF8String.fromString(DigestUtils.sha256Hex(input))
      case 384 =>
        UTF8String.fromString(DigestUtils.sha384Hex(input))
      case 512 =>
        UTF8String.fromString(DigestUtils.sha512Hex(input))
      case _ => null
    }
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val digestUtils = "org.apache.commons.codec.digest.DigestUtils"
    nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
      s"""
        if ($eval2 == 224) {
          try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-224");
            md.update($eval1);
            ${ev.primitive} = UTF8String.fromBytes(md.digest());
          } catch (java.security.NoSuchAlgorithmException e) {
            ${ev.isNull} = true;
          }
        } else if ($eval2 == 256 || $eval2 == 0) {
          ${ev.primitive} =
            UTF8String.fromString($digestUtils.sha256Hex($eval1));
        } else if ($eval2 == 384) {
          ${ev.primitive} =
            UTF8String.fromString($digestUtils.sha384Hex($eval1));
        } else if ($eval2 == 512) {
          ${ev.primitive} =
            UTF8String.fromString($digestUtils.sha512Hex($eval1));
        } else {
          ${ev.isNull} = true;
        }
      """
    })
  }
}

/**
 * A function that calculates a sha1 hash value and returns it as a hex string
 * For input of type [[BinaryType]] or [[StringType]]
  * 计算sha1哈希值并将其作为十六进制字符串返回的函数用于输入[[BinaryType]]或[[StringType]]类型的输入
 */
case class Sha1(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = StringType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(DigestUtils.shaHex(input.asInstanceOf[Array[Byte]]))

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    defineCodeGen(ctx, ev, c =>
      s"UTF8String.fromString(org.apache.commons.codec.digest.DigestUtils.shaHex($c))"
    )
  }
}

/**
 * A function that computes a cyclic redundancy check value and returns it as a bigint
 * For input of type [[BinaryType]]
  * 计算循环冗余校验值并将其作为bigint返回的函数对于[BinaryType]类型的输入
 */
case class Crc32(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = LongType

  override def inputTypes: Seq[DataType] = Seq(BinaryType)

  protected override def nullSafeEval(input: Any): Any = {
    val checksum = new CRC32
    checksum.update(input.asInstanceOf[Array[Byte]], 0, input.asInstanceOf[Array[Byte]].length)
    checksum.getValue
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val CRC32 = "java.util.zip.CRC32"
    nullSafeCodeGen(ctx, ev, value => {
      s"""
        $CRC32 checksum = new $CRC32();
        checksum.update($value, 0, $value.length);
        ${ev.primitive} = checksum.getValue();
      """
    })
  }
}
