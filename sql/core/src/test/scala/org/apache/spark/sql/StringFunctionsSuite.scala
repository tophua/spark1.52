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

package org.apache.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.Decimal


class StringFunctionsSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("string concat") {//字符串连接
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")
    /***
     *+---+---+----+
      |  a|  b|   c|
      +---+---+----+
      |  a|  b|null|
      +---+---+----+
     */
    df.show()
    checkAnswer(
      df.select(concat($"a", $"b"), concat($"a", $"b", $"c")),
      //注意如果有一个字段为null,连接为null
      Row("ab", null))

    checkAnswer(
        //表达式的形式字符串连接
      df.selectExpr("concat(a, b)", "concat(a, b, c)"),
      Row("ab", null))
  }

  test("string concat_ws") {//字符串连接
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")

    checkAnswer(//将多个输入字符串列成一个字符串列,使用给定的分隔符,如果字段null,则可以连接
      df.select(concat_ws("||", $"a", $"b", $"c")),
      Row("a||b"))

    checkAnswer(
        //表达式方式
      df.selectExpr("concat_ws('||', a, b, c)"),
      Row("a||b"))
  }

  test("string Levenshtein distance") {//字符串编辑距离
    val df = Seq(("kitten", "sitting"), ("frog", "fog")).toDF("l", "r")
    
    checkAnswer(df.select(levenshtein($"l", $"r")), Seq(Row(3), Row(1)))
    checkAnswer(df.selectExpr("levenshtein(l, r)"), Seq(Row(3), Row(1)))
  }

  test("string regex_replace / regex_extract") {//字符串正则表达式替换/提取
    val df = Seq(
      ("100-200", "(\\d+)-(\\d+)", "300"),
      ("100-200", "(\\d+)-(\\d+)", "400"),
      ("100-200", "(\\d+)", "400")).toDF("a", "b", "c")
    df.show()
    /**
     * +-------+-----------+---+
       |      a|          b|  c|
       +-------+-----------+---+
       |100-200|(\d+)-(\d+)|300|
       |100-200|(\d+)-(\d+)|400|
       |100-200|      (\d+)|400|
       +-------+-----------+---+
     */
    df.select(
          //正则表达式替换
        regexp_replace($"a", "(\\d+)", "num"),
        //正则表达式提取,1 提取正则表达式第一个匹配项
        regexp_extract($"a", "(\\d+)-(\\d+)", 1)).show()
     /**+---------------------------+-------------------------------+
        |regexp_replace(a,(\d+),num)|regexp_extract(a,(\d+)-(\d+),1)|
        +---------------------------+-------------------------------+
        |                    num-num|                            100|
        |                    num-num|                            100|
        |                    num-num|                            100|
        +---------------------------+-------------------------------+ **/
    checkAnswer(
      df.select(
          //正则表达式替换
        regexp_replace($"a", "(\\d+)", "num"),
        //正则表达式提取,1 提取第一个匹配项
        regexp_extract($"a", "(\\d+)-(\\d+)", 1)),
      Row("num-num", "100") :: Row("num-num", "100") :: Row("num-num", "100") :: Nil)

    // for testing the mutable state of the expression in code gen.
    //测试代码中表达式的可变状态
    // This is a hack way to enable the codegen, thus the codegen is enable by default,
    //这是一个黑客的方式使代码生成,代码生成是默认启用
    // it will still use the interpretProjection if projection followed by a LocalRelation,
    // hence we add a filter operator.因此,我们添加了一个过滤器操作符
    // See the optimizer rule `ConvertToLocalRelation`
      /**
       * isnotnull过虑掉a列数据行不为null
       * +-------+-----------+---+
         |      a|          b|  c|
         +-------+-----------+---+
         |100-200|(\d+)-(\d+)|300|
         |100-200|(\d+)-(\d+)|400|
         |100-200|      (\d+)|400|
         +-------+-----------+---+
       */
      df.filter("isnotnull(a)").show()
   /**+----------------------+----------------------+
      |'regexp_replace(a,b,c)|'regexp_extract(a,b,1)|
      +----------------------+----------------------+
      |                   300|                   100|
      |                   400|                   100|
      |               400-400|                   100|
      +----------------------+----------------------+**/
      df.filter("isnotnull(a)").selectExpr(
        //regexp_replaces该函数用一个指定的 替换匹配的模式,从而允许复杂的"搜索并替换"操作
        "regexp_replace(a, b, c)",
        //regexp_extract 将字符串subject按照pattern正则表达式的规则拆分,1匹配第一个开始,返回提取匹配项
        "regexp_extract(a, b, 1)").show()
    checkAnswer(
      df.filter("isnotnull(a)").selectExpr(
        "regexp_replace(a, b, c)",
        "regexp_extract(a, b, 1)"),
      Row("300", "100") :: Row("400", "100") :: Row("400-400", "100") :: Nil)
  }

  test("string ascii function") {//字符串ASCII码
    val df = Seq(("abc", "")).toDF("a", "b")
    checkAnswer(
      //字符串ASCII码
      df.select(ascii($"a"), ascii($"b")),
      Row(97, 0))

    checkAnswer(
      df.selectExpr("ascii(a)", "ascii(b)"),
      Row(97, 0))
  }

  test("string base64/unbase64 function") {//字符串Base64 / unbase64功能
    val bytes = Array[Byte](1, 2, 3, 4)
    val df = Seq((bytes, "AQIDBA==")).toDF("a", "b")
    checkAnswer(
      df.select(base64($"a"), unbase64($"b")),
      Row("AQIDBA==", bytes))

    checkAnswer(
      df.selectExpr("base64(a)", "unbase64(b)"),
      Row("AQIDBA==", bytes))
  }

  test("string / binary substring function") {//字符串/二进制截取函数
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    //非ASCII字符的代码不允许,所以我们禁用scalastyle这里
    //使用substring截取字符串
    val df = Seq(("1世3", Array[Byte](1, 2, 3, 4))).toDF("a", "b")
    //substring列名,从第几个开始,截取几位
    checkAnswer(df.select(substring($"a", 1, 2)), Row("1世"))
    checkAnswer(df.select(substring($"b", 2, 2)), Row(Array[Byte](2,3)))
    checkAnswer(df.selectExpr("substring(a, 1, 2)"), Row("1世"))
    // scalastyle:on
  }

  test("string encode/decode function") {//字符串编码/解码功能
    val bytes = Array[Byte](-27, -92, -89, -27, -115, -125, -28, -72, -106, -25, -107, -116)
    // scalastyle:off
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    //非ASCII字符的代码不允许,所以我们禁用scalastyle这里
    val df = Seq(("大千世界", "utf-8", bytes)).toDF("a", "b", "c")
    /**
    +--------------------+---------------+
    |     encode(a,utf-8)|decode(c,utf-8)|
    +--------------------+---------------+
    |[-27, -92, -89, -...|        大千世界|
    +--------------------+---------------+
    **/
    df.select(encode($"a", "utf-8"), decode($"c", "utf-8")).show()
    checkAnswer(
      df.select(encode($"a", "utf-8"), decode($"c", "utf-8")),
      Row(bytes, "大千世界"))

    checkAnswer(
      df.selectExpr("encode(a, 'utf-8')", "decode(c, 'utf-8')"),
      Row(bytes, "大千世界"))
    // scalastyle:on
  }

  test("string translate") {//字符替换函数
    val df = Seq(("translate", "")).toDF("a", "b")
    
    checkAnswer(df.select(translate($"a", "rnlt", "123")), Row("1a2s3ae"))
    checkAnswer(df.selectExpr("""translate(a, "rnlt", "")"""), Row("asae"))
  }

  test("string trim functions") {//字符串空格截取函数
    val df = Seq(("  example  ", "")).toDF("a", "b")
    //截取空格
    checkAnswer(
      df.select(ltrim($"a"), rtrim($"a"), trim($"a")),
      Row("example  ", "  example", "example"))

    checkAnswer(
      df.selectExpr("ltrim(a)", "rtrim(a)", "trim(a)"),
      Row("example  ", "  example", "example"))
  }

  test("string formatString function") {//字符串格式化函数
    val df = Seq(("aa%d%s", 123, "cc")).toDF("a", "b", "c")

    checkAnswer(
        //%d表示数字,%s表示字符串
      df.select(format_string("aa%d%s", $"b", $"c")),
      Row("aa123cc"))

    checkAnswer(
      df.selectExpr("printf(a, b, c)"),
      Row("aa123cc"))
  }

  test("soundex function") {//Soundex算法函数
    val df = Seq(("MARY", "SU")).toDF("l", "r")
    checkAnswer(
      df.select(soundex($"l"), soundex($"r")), Row("M600", "S000"))

    checkAnswer(
      df.selectExpr("SoundEx(l)", "SoundEx(r)"), Row("M600", "S000"))
  }

  test("string instr function") {//返回要截取的字符串在源字符串中的位置
    val df = Seq(("aaads", "aa", "zz")).toDF("a", "b", "c")

    checkAnswer(
        //返回要截取的字符串在源字符串中开始的位置
      df.select(instr($"a", "aa")),
      Row(1))

    checkAnswer(
      df.selectExpr("instr(a, b)"),
      Row(1))
  }

  test("string substring_index function") {//截取子字符串索引函数
    val df = Seq(("www.apache.org", ".", "zz")).toDF("a", "b", "c")
  
    checkAnswer(
        //截取子字符串索引函数,2匹配索引截取的位置
      df.select(substring_index($"a", ".", 2)),
      Row("www.apache"))
    checkAnswer(
      df.selectExpr("substring_index(a, '.', 2)"),
      Row("www.apache")
    )
  }

  test("string locate function") {//字符串查找匹配定位函数
    val df = Seq(("aaads", "aa", "zz", 1)).toDF("a", "b", "c", "d")

    checkAnswer(
      //locate 确定…的位置
      df.select(locate("aa", $"a"), locate("aa", $"a", 1)),
      Row(1, 2))

    checkAnswer(
      df.selectExpr("locate(b, a)", "locate(b, a, d)"),
      Row(1, 2))
  }

  test("string padding functions") {//字符串填充函数
    val df = Seq(("hi", 5, "??")).toDF("a", "b", "c")

    checkAnswer(
      df.select(lpad($"a", 1, "c"), lpad($"a", 5, "??"), rpad($"a", 1, "c"), rpad($"a", 5, "??")),
      Row("h", "???hi", "h", "hi???"))

    checkAnswer(
      df.selectExpr("lpad(a, b, c)", "rpad(a, b, c)", "lpad(a, 1, c)", "rpad(a, 1, c)"),
      Row("???hi", "hi???", "h", "h"))
  }

  test("string repeat function") {//字符串重复函数
    val df = Seq(("hi", 2)).toDF("a", "b")

    checkAnswer(
      df.select(repeat($"a", 2)),
      Row("hihi"))

    checkAnswer(
      df.selectExpr("repeat(a, 2)", "repeat(a, b)"),
      Row("hihi", "hihi"))
  }

  test("string reverse function") {//字符串反转函数
    val df = Seq(("hi", "hhhi")).toDF("a", "b")

    checkAnswer(
      df.select(reverse($"a"), reverse($"b")),
      Row("ih", "ihhh"))

    checkAnswer(
      df.selectExpr("reverse(b)"),
      Row("ihhh"))
  }

  test("string space function") {//字符串空格函数
    val df = Seq((2, 3)).toDF("a", "b")

    checkAnswer(
      df.selectExpr("space(b)"),
      Row("   "))
  }

  test("string split function") {//字符串分隔函数,返回数组
    val df = Seq(("aa2bb3cc", "[1-9]+")).toDF("a", "b")
    
    checkAnswer(
      df.select(split($"a", "[1-9]+")),
      Row(Seq("aa", "bb", "cc")))

    checkAnswer(
      df.selectExpr("split(a, '[1-9]+')"),
      Row(Seq("aa", "bb", "cc")))
  }

  test("string / binary length function") {//字符串/二进制长度函数
    val df = Seq(("123", Array[Byte](1, 2, 3, 4), 123)).toDF("a", "b", "c")
    checkAnswer(
      df.select(length($"a"), length($"b")),
      Row(3, 4))

    checkAnswer(
      df.selectExpr("length(a)", "length(b)"),
      Row(3, 4))

    intercept[AnalysisException] {
      checkAnswer(//int类型的参数是不可接受的
        df.selectExpr("length(c)"), // int type of the argument is unacceptable
        Row("5.0000"))
    }
  }

  test("initcap function") {//首字母大写函数
    val df = Seq(("ab", "a B")).toDF("l", "r")
    checkAnswer(
      df.select(initcap($"l"), initcap($"r")), Row("Ab", "A B"))

    checkAnswer(//全部转换大写字母函数
      df.selectExpr("InitCap(l)", "InitCap(r)"), Row("Ab", "A B"))
  }

  test("number format function") {//数字格式化函数
    val tuple =
      ("aa", 1.asInstanceOf[Byte], 2.asInstanceOf[Short],
        3.13223f, 4, 5L, 6.48173d, Decimal(7.128381))
    val df =
      Seq(tuple)
        .toDF(
          "a", // string "aa"
          "b", // byte    1
          "c", // short   2
          "d", // float   3.13223f
          "e", // integer 4
          "f", // long    5L
          "g", // double  6.48173d
          "h") // decimal 7.128381

    checkAnswer(
       //数字格式4个零
      df.select(format_number($"f", 4)),
      Row("5.0000"))

    checkAnswer(
        //将第一个参数转换为整数
      df.selectExpr("format_number(b, e)"), // convert the 1st argument to integer
      Row("1.0000"))

    checkAnswer(//将第一个参数转换为整数
      df.selectExpr("format_number(c, e)"), // convert the 1st argument to integer
      Row("2.0000"))

    checkAnswer(//将第一个参数转换为double类型
      df.selectExpr("format_number(d, e)"), // convert the 1st argument to double
      Row("3.1322"))

    checkAnswer(//不转换任何东西
      df.selectExpr("format_number(e, e)"), // not convert anything
      Row("4.0000"))

    checkAnswer(//不转换任何东西
      df.selectExpr("format_number(f, e)"), // not convert anything
      Row("5.0000"))

    checkAnswer(//不转换任何东西
      df.selectExpr("format_number(g, e)"), // not convert anything
      Row("6.4817"))

    checkAnswer(//不转换任何东西
      df.selectExpr("format_number(h, e)"), // not convert anything
      Row("7.1284"))

    intercept[AnalysisException] {
      checkAnswer(
        df.selectExpr("format_number(a, e)"), // string type of the 1st argument is unacceptable
        Row("5.0000"))
    }

    intercept[AnalysisException] {
      checkAnswer(//字符串类型的第一个参数是不可接受的
        df.selectExpr("format_number(e, g)"), // decimal type of the 2nd argument is unacceptable
        Row("5.0000"))
    }

    // for testing the mutable state of the expression in code gen.
    //测试代码中生成可变状态表达式
    // This is a hack way to enable the codegen, thus the codegen is enable by default,
    // it will still use the interpretProjection if projection follows by a LocalRelation,
    // hence we add a filter operator.
    // See the optimizer rule `ConvertToLocalRelation`
    val df2 = Seq((5L, 4), (4L, 3), (4L, 3), (4L, 3), (3L, 2)).toDF("a", "b")
    checkAnswer(
      df2.filter("b>0").selectExpr("format_number(a, b)"),
      Row("5.0000") :: Row("4.000") :: Row("4.000") :: Row("4.000") :: Row("3.00") :: Nil)
  }
}
