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

import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.slf4j.{Logger, LoggerFactory}
import org.slf4j.impl.StaticLoggerBinder

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

/**
 * :: DeveloperApi ::
 * Utility trait for classes that want to log data. Creates a SLF4J logger for the class and allows
 * logging messages at different levels using methods that only evaluate parameters lazily if the
 * log level is enabled.
  * 要记录数据的类的实用特性,为该类创建SLF4J记录器,并允许使用仅在启用日志级别的情况下,懒惰地评估参数的方法在不同级别记录消息。
 *
 * NOTE: DO NOT USE this class outside of Spark. It is intended as an internal utility.
 *       This will likely be changed or removed in future releases.
  *       注意：请勿在Spark之外使用此类,它是作为一个内部实用程序,这在将来的版本中可能会被更改或删除。
 */
@DeveloperApi
trait Logging {
  // Make the log field transient so that objects with Logging can
  // be serialized and used on another machine
  //使用日志字段进行日志字段的临时,以便用日志记录可被序列化并用另一台机器上
  @transient private var log_ : Logger = null

  // Method to get the logger name for this object
  //获得日志对象名称的方法
  protected def logName = {
    // Ignore trailing $'s in the class names for Scala objects
    //忽略后面的$在Scala对象类的名称
    this.getClass.getName.stripSuffix("$")
  }

  // Method to get or create the logger for this object
  //获取或创建日志对象的方法
  protected def log: Logger = {
    if (log_ == null) {
      initializeIfNecessary()
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  // Log methods that take only a String
  //只需要一个字符串的日志方法
  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  //日志的方法,以Throwables(异常/错误)
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def isTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  private def initializeIfNecessary() {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging()
        }
      }
    }
  }

  private def initializeLogging() {
    // Don't use a logger in here, as this is itself occurring during initialization of a logger
    //不要在这里使用日志,这本身是一个初始化期间产生的日志
    // If Log4j 1.2 is being used, but is not initialized, load a default properties file
    //如果正在使用log4j 1.2,但未初始化,加载一个默认属性文件
    val binderClass = StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    // This distinguishes the log4j 1.2 binding, currently
    //目前这是区分1.2结合log4j
    // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
    // org.apache.logging.slf4j.Log4jLoggerFactory
    val usingLog4j12 = "org.slf4j.impl.Log4jLoggerFactory".equals(binderClass)
    if (usingLog4j12) {
      val log4j12Initialized = LogManager.getRootLogger.getAllAppenders.hasMoreElements
      if (!log4j12Initialized) {
        // scalastyle:off println
        if (Utils.isInInterpreter) {
          val replDefaultLogProps = "org/apache/spark/log4j-defaults-repl.properties"
          Option(Utils.getSparkClassLoader.getResource(replDefaultLogProps)) match {
            case Some(url) =>
              PropertyConfigurator.configure(url)
              System.err.println(s"Using Spark's repl log4j profile: $replDefaultLogProps")
              System.err.println("To adjust logging level use sc.setLogLevel(\"INFO\")")
            case None =>
              System.err.println(s"Spark was unable to load $replDefaultLogProps")
          }
        } else {
          val defaultLogProps = "org/apache/spark/log4j-defaults.properties"
          Option(Utils.getSparkClassLoader.getResource(defaultLogProps)) match {
            case Some(url) =>
              PropertyConfigurator.configure(url)
              System.err.println(s"Using Spark's default log4j profile: $defaultLogProps")
            case None =>
              System.err.println(s"Spark was unable to load $defaultLogProps")
          }
        }
        // scalastyle:on println
      }
    }
    Logging.initialized = true

    // Force a call into slf4j to initialize it. Avoids this happening from multiple threads
    // and triggering this: http://mailman.qos.ch/pipermail/slf4j-dev/2010-April/002956.html
    log
  }
}

private object Logging {
  @volatile private var initialized = false
  val initLock = new Object()
  try {
    // We use reflection here to handle the case where users remove the
    // slf4j-to-jul bridge order to route their logs to JUL.
    val bridgeClass = Utils.classForName("org.slf4j.bridge.SLF4JBridgeHandler")
    bridgeClass.getMethod("removeHandlersForRootLogger").invoke(null)
    val installed = bridgeClass.getMethod("isInstalled").invoke(null).asInstanceOf[Boolean]
    if (!installed) {
      bridgeClass.getMethod("install").invoke(null)
    }
  } catch {
    case e: ClassNotFoundException => // can't log anything yet so just fail silently
  }
}
