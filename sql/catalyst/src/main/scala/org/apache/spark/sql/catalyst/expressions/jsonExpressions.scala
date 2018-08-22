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

import java.io.{StringWriter, ByteArrayOutputStream}

import com.fasterxml.jackson.core._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{StringType, DataType}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.parsing.combinator.RegexParsers

private[this] sealed trait PathInstruction
private[this] object PathInstruction {
  private[expressions] case object Subscript extends PathInstruction
  private[expressions] case object Wildcard extends PathInstruction
  private[expressions] case object Key extends PathInstruction
  private[expressions] case class Index(index: Long) extends PathInstruction
  private[expressions] case class Named(name: String) extends PathInstruction
}

private[this] sealed trait WriteStyle
private[this] object WriteStyle {
  private[expressions] case object RawStyle extends WriteStyle
  private[expressions] case object QuotedStyle extends WriteStyle
  private[expressions] case object FlattenStyle extends WriteStyle
}

private[this] object JsonPathParser extends RegexParsers {
  import PathInstruction._

  def root: Parser[Char] = '$'

  def long: Parser[Long] = "\\d+".r ^? {
    case x => x.toLong
  }

  // parse `[*]` and `[123]` subscripts
  //解析`[*]`和`[123]`下标
  def subscript: Parser[List[PathInstruction]] =
    for {
      operand <- '[' ~> ('*' ^^^ Wildcard | long ^^ Index) <~ ']'
    } yield {
      Subscript :: operand :: Nil
    }

  // parse `.name` or `['name']` child expressions
  //解析`.name`或`['name']`子表达式
  def named: Parser[List[PathInstruction]] =
    for {
      name <- '.' ~> "[^\\.\\[]+".r | "[\\'" ~> "[^\\'\\?]+" <~ "\\']"
    } yield {
      Key :: Named(name) :: Nil
    }

  // child wildcards: `..`, `.*` or `['*']`
  def wildcard: Parser[List[PathInstruction]] =
    (".*" | "['*']") ^^^ List(Wildcard)

  def node: Parser[List[PathInstruction]] =
    wildcard |
      named |
      subscript

  val expression: Parser[List[PathInstruction]] = {
    phrase(root ~> rep(node) ^^ (x => x.flatten))
  }

  def parse(str: String): Option[List[PathInstruction]] = {
    this.parseAll(expression, str) match {
      case Success(result, _) =>
        Some(result)

      case NoSuccess(msg, next) =>
        None
    }
  }
}

private[this] object GetJsonObject {
  private val jsonFactory = new JsonFactory()

  // Enabled for Hive compatibility
  //启用了Hive兼容性
  jsonFactory.enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
}

/**
 * Extracts json object from a json string based on json path specified, and returns json string
 * of the extracted json object. It will return null if the input json string is invalid.
  * 基于指定的json路径从json字符串中提取json对象,并返回提取的json对象的json字符串,
  * 如果输入json字符串无效,它将返回null。
 */
case class GetJsonObject(json: Expression, path: Expression)
  extends BinaryExpression with ExpectsInputTypes with CodegenFallback {

  import GetJsonObject._
  import PathInstruction._
  import WriteStyle._
  import com.fasterxml.jackson.core.JsonToken._

  override def left: Expression = json
  override def right: Expression = path
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)
  override def dataType: DataType = StringType
  override def prettyName: String = "get_json_object"

  @transient private lazy val parsedPath = parsePath(path.eval().asInstanceOf[UTF8String])

  override def eval(input: InternalRow): Any = {
    val jsonStr = json.eval(input).asInstanceOf[UTF8String]
    if (jsonStr == null) {
      return null
    }

    val parsed = if (path.foldable) {
      parsedPath
    } else {
      parsePath(path.eval(input).asInstanceOf[UTF8String])
    }

    if (parsed.isDefined) {
      try {
        val parser = jsonFactory.createParser(jsonStr.getBytes)
        val output = new ByteArrayOutputStream()
        val generator = jsonFactory.createGenerator(output, JsonEncoding.UTF8)
        parser.nextToken()
        val matched = evaluatePath(parser, generator, RawStyle, parsed.get)
        generator.close()
        if (matched) {
          UTF8String.fromBytes(output.toByteArray)
        } else {
          null
        }
      } catch {
        case _: JsonProcessingException => null
      }
    } else {
      null
    }
  }

  private def parsePath(path: UTF8String): Option[List[PathInstruction]] = {
    if (path != null) {
      JsonPathParser.parse(path.toString)
    } else {
      None
    }
  }

  // advance to the desired array index, assumes to start at the START_ARRAY token
  //前进到所需的数组索引,假定从START_ARRAY标记开始
  private def arrayIndex(p: JsonParser, f: () => Boolean): Long => Boolean = {
    case _ if p.getCurrentToken == END_ARRAY =>
      // terminate, nothing has been written
      false

    case 0 =>
      // we've reached the desired index
      //我们达到了预期的指数
      val dirty = f()

      while (p.nextToken() != END_ARRAY) {
        // advance the token stream to the end of the array
        //将令牌流推进到数组的末尾
        p.skipChildren()
      }

      dirty

    case i if i > 0 =>
      // skip this token and evaluate the next
      p.skipChildren()
      p.nextToken()
      arrayIndex(p, f)(i - 1)
  }

  /**
   * Evaluate a list of JsonPath instructions, returning a bool that indicates if any leaf nodes
   * have been written to the generator
    * 评估JsonPath指令列表,返回一个bool,指示是否有任何叶节点已写入生成器
   */
  private def evaluatePath(
      p: JsonParser,
      g: JsonGenerator,
      style: WriteStyle,
      path: List[PathInstruction]): Boolean = {
    (p.getCurrentToken, path) match {
      case (VALUE_STRING, Nil) if style == RawStyle =>
        // there is no array wildcard or slice parent, emit this string without quotes
        //没有数组通配符或切片父级,不带引号发出此字符串
        if (p.hasTextCharacters) {
          g.writeRaw(p.getTextCharacters, p.getTextOffset, p.getTextLength)
        } else {
          g.writeRaw(p.getText)
        }
        true

      case (START_ARRAY, Nil) if style == FlattenStyle =>
        // flatten this array into the parent
        var dirty = false
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, style, Nil)
        }
        dirty

      case (_, Nil) =>
        // general case: just copy the child tree verbatim
        //一般情况：只需逐字复制子树
        g.copyCurrentStructure(p)
        true

      case (START_OBJECT, Key :: xs) =>
        var dirty = false
        while (p.nextToken() != END_OBJECT) {
          if (dirty) {
            // once a match has been found we can skip other fields
            //一旦找到匹配,我们可以跳过其他字段
            p.skipChildren()
          } else {
            dirty = evaluatePath(p, g, style, xs)
          }
        }
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: Subscript :: Wildcard :: xs) =>
        // special handling for the non-structure preserving double wildcard behavior in Hive
        //对Hive中非结构保留双通配符行为的特殊处理
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          dirty |= evaluatePath(p, g, FlattenStyle, xs)
        }
        g.writeEndArray()
        dirty

      case (START_ARRAY, Subscript :: Wildcard :: xs) if style != QuotedStyle =>
        // retain Flatten, otherwise use Quoted... cannot use Raw within an array
        //保留Flatten，否则使用Quoted ...不能在数组中使用Raw
        val nextStyle = style match {
          case RawStyle => QuotedStyle
          case FlattenStyle => FlattenStyle
          case QuotedStyle => throw new IllegalStateException()
        }

        // temporarily buffer child matches, the emitted json will need to be
        // modified slightly if there is only a single element written
        //暂时缓冲子匹配,如果只写入一个元素,则需要稍微修改发出的json
        val buffer = new StringWriter()
        val flattenGenerator = jsonFactory.createGenerator(buffer)
        flattenGenerator.writeStartArray()

        var dirty = 0
        while (p.nextToken() != END_ARRAY) {
          // track the number of array elements and only emit an outer array if
          // we've written more than one element, this matches Hive's behavior
          //跟踪数组元素的数量,如果我们编写了多个元素,则只发出一个外部数组,这与Hive的行为相匹配
          dirty += (if (evaluatePath(p, flattenGenerator, nextStyle, xs)) 1 else 0)
        }
        flattenGenerator.writeEndArray()
        flattenGenerator.close()

        val buf = buffer.getBuffer
        if (dirty > 1) {
          g.writeRawValue(buf.toString)
        } else if (dirty == 1) {
          // remove outer array tokens
          g.writeRawValue(buf.substring(1, buf.length()-1))
        } // else do not write anything

        dirty > 0

      case (START_ARRAY, Subscript :: Wildcard :: xs) =>
        var dirty = false
        g.writeStartArray()
        while (p.nextToken() != END_ARRAY) {
          // wildcards can have multiple matches, continually update the dirty count
          //通配符可以有多个匹配,不断更新脏计数
          dirty |= evaluatePath(p, g, QuotedStyle, xs)
        }
        g.writeEndArray()

        dirty

      case (START_ARRAY, Subscript :: Index(idx) :: (xs@Subscript :: Wildcard :: _)) =>
        p.nextToken()
        // we're going to have 1 or more results, switch to QuotedStyle
        //我们将有一个或多个结果,切换到QuotedStyle
        arrayIndex(p, () => evaluatePath(p, g, QuotedStyle, xs))(idx)

      case (START_ARRAY, Subscript :: Index(idx) :: xs) =>
        p.nextToken()
        arrayIndex(p, () => evaluatePath(p, g, style, xs))(idx)

      case (FIELD_NAME, Named(name) :: xs) if p.getCurrentName == name =>
        // exact field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case (FIELD_NAME, Wildcard :: xs) =>
        // wildcard field match
        p.nextToken()
        evaluatePath(p, g, style, xs)

      case _ =>
        p.skipChildren()
        false
    }
  }
}
