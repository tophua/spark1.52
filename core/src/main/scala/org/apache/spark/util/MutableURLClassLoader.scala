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

package org.apache.spark.util

import java.net.{URLClassLoader, URL}
import java.util.Enumeration
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

/**
 * URL class loader that exposes the `addURL` and `getURLs` methods in URLClassLoader.
  *在URLClassLoader中显示“addURL”和“getURLs”方法的URL类加载器
  * URLClassLoader是ClassLoader的子类,它用于从指向JAR文件和目录的URL的搜索路径加载类和资源
  * 也就是说,通过URLClassLoader就可以加载指定jar中的class到内存中。
 */
private[spark] class MutableURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  override def addURL(url: URL): Unit = {
    super.addURL(url)
  }

  override def getURLs(): Array[URL] = {
    super.getURLs()
  }

}

/**
 * A mutable class loader that gives preference to its own URLs over the parent class loader
 * when loading classes and resources.
  * 一个可变类加载器,当加载类和资源时,可以通过父类加载器优先选择自己的URL.
 */
private[spark] class ChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends MutableURLClassLoader(urls, null) {

  private val parentClassLoader = new ParentClassLoader(parent)

  /**
   * Used to implement fine-grained class loading locks similar to what is done by Java 7. This
   * prevents deadlock issues when using non-hierarchical class loaders.
    * 用于实现类似于Java 7所做的细粒度类加载锁定防止在使用非层次化类加载器时出现死锁问题。
   *
   * Note that due to some issues with implementing class loaders in
   * Scala, Java 7's `ClassLoader.registerAsParallelCapable` method is not called.
    * 请注意，由于实现类加载器的一些问题Scala，Java 7的`ClassLoader.registerAsParallelCapable`方法不被调用
   */
  private val locks = new ConcurrentHashMap[String, Object]()

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    var lock = locks.get(name)
    if (lock == null) {
      val newLock = new Object()
      lock = locks.putIfAbsent(name, newLock)
      if (lock == null) {
        lock = newLock
      }
    }

    lock.synchronized {
      try {
        super.loadClass(name, resolve)
      } catch {
        case e: ClassNotFoundException =>
          parentClassLoader.loadClass(name, resolve)
      }
    }
  }

  override def getResource(name: String): URL = {
    val url = super.findResource(name)
    val res = if (url != null) url else parentClassLoader.getResource(name)
    res
  }

  override def getResources(name: String): Enumeration[URL] = {
    val urls = super.findResources(name)
    val res =
      if (urls != null && urls.hasMoreElements()) {
        urls
      } else {
        parentClassLoader.getResources(name)
      }
    res
  }

  override def addURL(url: URL) {
    super.addURL(url)
  }

}
