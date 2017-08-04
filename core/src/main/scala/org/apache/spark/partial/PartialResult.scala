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

package org.apache.spark.partial

import org.apache.spark.annotation.Experimental

@Experimental
class PartialResult[R](initialVal: R, isFinal: Boolean) {
  private var finalValue: Option[R] = if (isFinal) Some(initialVal) else None
  private var failure: Option[Exception] = None
  private var completionHandler: Option[R => Unit] = None
  private var failureHandler: Option[Exception => Unit] = None

  def initialValue: R = initialVal

  def isInitialValueFinal: Boolean = isFinal

  /**
   * Blocking method to wait for and return the final value.
    * 阻止方法等待并返回最终值
   */
  def getFinalValue(): R = synchronized {
    while (finalValue.isEmpty && failure.isEmpty) {
      this.wait()
    }
    if (finalValue.isDefined) {
      return finalValue.get
    } else {
      throw failure.get
    }
  }

  /**
   * Set a handler to be called when this PartialResult completes. Only one completion handler
   * is supported per PartialResult.
    * 设置此PartialResult完成时调用的处理程序,每个PartialResult只支持一个完成处理程序
   */
  def onComplete(handler: R => Unit): PartialResult[R] = synchronized {
    if (completionHandler.isDefined) {
      throw new UnsupportedOperationException("onComplete cannot be called twice")
    }
    completionHandler = Some(handler)
    if (finalValue.isDefined) {
      // We already have a final value, so let's call the handler
      //我们已经有一个最终的值,所以我们来调用处理程序
      handler(finalValue.get)
    }
    return this
  }

  /**
   * Set a handler to be called if this PartialResult's job fails. Only one failure handler
   * is supported per PartialResult.
    * 如果此部分结果作业失败,请设置要调用的处理程序,每个部分结果只支持一个故障处理程序
   */
  def onFail(handler: Exception => Unit) {
    synchronized {
      if (failureHandler.isDefined) {
        throw new UnsupportedOperationException("onFail cannot be called twice")
      }
      failureHandler = Some(handler)
      if (failure.isDefined) {
        // We already have a failure, so let's call the handler
        //我们已经失败了,所以让我们来调用处理程序
        handler(failure.get)
      }
    }
  }

  /**
   * Transform this PartialResult into a PartialResult of type T.
    * 将此部分结果转换为类型T的部分结果
   */
  def map[T](f: R => T) : PartialResult[T] = {
    new PartialResult[T](f(initialVal), isFinal) {
       override def getFinalValue() : T = synchronized {
         f(PartialResult.this.getFinalValue())
       }
       override def onComplete(handler: T => Unit): PartialResult[T] = synchronized {
         PartialResult.this.onComplete(handler.compose(f)).map(f)
       }
      override def onFail(handler: Exception => Unit) {
        synchronized {
          PartialResult.this.onFail(handler)
        }
      }
      override def toString : String = synchronized {
        PartialResult.this.getFinalValueInternal() match {
          case Some(value) => "(final: " + f(value) + ")"
          case None => "(partial: " + initialValue + ")"
        }
      }
      def getFinalValueInternal(): Option[T] = PartialResult.this.getFinalValueInternal().map(f)
    }
  }

  private[spark] def setFinalValue(value: R) {
    synchronized {
      if (finalValue.isDefined) {
        throw new UnsupportedOperationException("setFinalValue called twice on a PartialResult")
      }
      finalValue = Some(value)
      // Call the completion handler if it was set
      completionHandler.foreach(h => h(value))
      // Notify any threads that may be calling getFinalValue()
      this.notifyAll()
    }
  }

  private def getFinalValueInternal() = finalValue

  private[spark] def setFailure(exception: Exception) {
    synchronized {
      if (failure.isDefined) {
        throw new UnsupportedOperationException("setFailure called twice on a PartialResult")
      }
      failure = Some(exception)
      // Call the failure handler if it was set
      //调用失败处理程序(如果已设置)
      failureHandler.foreach(h => h(exception))
      // Notify any threads that may be calling getFinalValue()
      //通知可能调用getFinalValue（）的任何线程
      this.notifyAll()
    }
  }

  override def toString: String = synchronized {
    finalValue match {
      case Some(value) => "(final: " + value + ")"
      case None => "(partial: " + initialValue + ")"
    }
  }
}
