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

package org.apache.spark

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream}
import java.net.{URI, URL}
import java.nio.file.Paths
import java.util.jar.{JarEntry, JarOutputStream}

import scala.collection.JavaConversions._

import com.google.common.base.Charsets.UTF_8
import com.google.common.io.{ByteStreams, Files}
import javax.tools.{JavaFileObject, SimpleJavaFileObject, ToolProvider}

import org.apache.spark.util.Utils

/**
 * Utilities for tests. Included in main codebase since it's used by multiple
 * projects.实用测试,包含在主代码库中,因为它被多个项目使用
 *
 * TODO: See if we can move this to the test codebase by specifying
 * test dependencies between projects.
  * TODO：看看我们是否可以通过指定项目之间的测试依赖关系将其移动到测试代码库。
 */
private[spark] object TestUtils {

  /**
   * Create a jar that defines classes with the given names.
    * 创建一个定义具有给定名称的类的jar,
   *
   * Note: if this is used during class loader tests, class names should be unique
   * in order to avoid interference between tests.
    * 注意：如果这是在类加载器测试期间使用的,则类名应该是唯一的,以避免测试之间的干扰。
   */
  def createJarWithClasses(
      classNames: Seq[String],
      toStringValue: String = "",
      classNamesWithBase: Seq[(String, String)] = Seq(),
      classpathUrls: Seq[URL] = Seq()): URL = {
    val tempDir = Utils.createTempDir()
    val files1 = for (name <- classNames) yield {
      createCompiledClass(name, tempDir, toStringValue, classpathUrls = classpathUrls)
    }
    val files2 = for ((childName, baseName) <- classNamesWithBase) yield {
      createCompiledClass(childName, tempDir, toStringValue, baseName, classpathUrls)
    }
    val jarFile = new File(tempDir, "testJar-%s.jar".format(System.currentTimeMillis()))
    createJar(files1 ++ files2, jarFile)
  }

  /**
   * Create a jar file containing multiple files. The `files` map contains a mapping of
   * file names in the jar file to their contents.
    * 创建一个包含多个jar文件的文件,`files`映射包含jar文件中的文件名与其内容的映射。
   */
  def createJarWithFiles(files: Map[String, String], dir: File = null): URL = {
    //注意Null和Option配合使用,File默认值
    val tempDir = Option(dir).getOrElse(Utils.createTempDir())

    //创建testJar.jar,目录位置tempDir
    val jarFile = File.createTempFile("testJar", ".jar", tempDir)
    //JarOutputStream文件输出
    val jarStream = new JarOutputStream(new FileOutputStream(jarFile))
    files.foreach { case (k, v) =>
      val entry = new JarEntry(k)
      //将ZIP条目列表写入输出流
      jarStream.putNextEntry(entry)
      //将输入流中的所有字节复制到输出流,不关闭流。
      ByteStreams.copy(new ByteArrayInputStream(v.getBytes(UTF_8)), jarStream)
    }
    jarStream.close()
    jarFile.toURI.toURL
  }

  /**
   * Create a jar file that contains this set of files. All files will be located in the specified
   * directory or at the root of the jar.
    * 创建一个包含这组文件的jar文件,所有文件将位于指定的目录或jar的根目录下。
   */
  def createJar(files: Seq[File], jarFile: File, directoryPrefix: Option[String] = None): URL = {
    val jarFileStream = new FileOutputStream(jarFile)
    val jarStream = new JarOutputStream(jarFileStream, new java.util.jar.Manifest())

    for (file <- files) {
      val jarEntry = new JarEntry(Paths.get(directoryPrefix.getOrElse(""), file.getName).toString)
      jarStream.putNextEntry(jarEntry)

      val in = new FileInputStream(file)
      ByteStreams.copy(in, jarStream)
      in.close()
    }
    jarStream.close()
    jarFileStream.close()

    jarFile.toURI.toURL
  }

  // Adapted from the JavaCompiler.java doc examples
  //改编自JavaCompiler.java doc示例
  private val SOURCE = JavaFileObject.Kind.SOURCE
  private def createURI(name: String) = {
    URI.create(s"string:///${name.replace(".", "/")}${SOURCE.extension}")
  }

  private[spark] class JavaSourceFromString(val name: String, val code: String)
    extends SimpleJavaFileObject(createURI(name), SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): String = code
  }

  /**
    * Creates a compiled class with the source file. Class file will be placed in destDir.
    * 使用源文件创建一个编译的类,类文件将被放在destDir中
    * */
  def createCompiledClass(
      className: String,
      destDir: File,
      sourceFile: JavaSourceFromString,
      classpathUrls: Seq[URL]): File = {
    val compiler = ToolProvider.getSystemJavaCompiler

    // Calling this outputs a class file in pwd. It's easier to just rename the files than
    // build a custom FileManager that controls the output location.
    //调用它会输出pwd中的类文件, 只需重新命名文件,就可以建立一个控制输出位置的自定义FileManager,
    val options = if (classpathUrls.nonEmpty) {
      Seq("-classpath", classpathUrls.map { _.getFile }.mkString(File.pathSeparator))
    } else {
      Seq()
    }
    compiler.getTask(null, null, null, options, null, Seq(sourceFile)).call()

    val fileName = className + ".class"
    val result = new File(fileName)
    assert(result.exists(), "Compiled file not found: " + result.getAbsolutePath())
    val out = new File(destDir, fileName)

    // renameTo cannot handle in and out files in different filesystems
    // use google's Files.move instead
    //renameTo不能处理在不同的文件系统使用谷歌的files.move代替文件
    Files.move(result, out)
    assert(out.exists(), "Destination file not moved: " + out.getAbsolutePath())
    out
  }

  /**
    * Creates a compiled class with the given name. Class file will be placed in destDir.
    * 创建一个编译的类的名字,类文件将被放置在DestDir
    * */
  def createCompiledClass(
      className: String,
      destDir: File,
      toStringValue: String = "",
      baseClass: String = null,
      classpathUrls: Seq[URL] = Seq()): File = {
    val extendsText = Option(baseClass).map { c => s" extends ${c}" }.getOrElse("")
    val sourceFile = new JavaSourceFromString(className,
      "public class " + className + extendsText + " implements java.io.Serializable {" +
      "  @Override public String toString() { return \"" + toStringValue + "\"; }}")
    createCompiledClass(className, destDir, sourceFile, classpathUrls)
  }
}
