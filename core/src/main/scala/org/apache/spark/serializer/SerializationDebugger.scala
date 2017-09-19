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

import java.io._
import java.lang.reflect.{Field, Method}
import java.security.AccessController

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.Logging

private[spark] object SerializationDebugger extends Logging {

  /**
   * Improve the given NotSerializableException with the serialization path leading from the given
   * object to the problematic object. This is turned off automatically if
   * `sun.io.serialization.extendedDebugInfo` flag is turned on for the JVM.
    * 使用从给定对象引导到有问题的对象的序列化路径来改进给定的NotSerializableException,
    * 如果为JVM启用了“sun.io.serialization.extendedDebugInfo”标志，这将自动关闭。
   */
  def improveException(obj: Any, e: NotSerializableException): NotSerializableException = {
    if (enableDebugging && reflect != null) {
      try {
        new NotSerializableException(
          e.getMessage + "\nSerialization stack:\n" + find(obj).map("\t- " + _).mkString("\n"))
      } catch {
        case NonFatal(t) =>
          // Fall back to old exception
          logWarning("Exception in serialization debugger", t)
          e
      }
    } else {
      e
    }
  }

  /**
   * Find the path leading to a not serializable object. This method is modeled after OpenJDK's
   * serialization mechanism, and handles the following cases:
    * 找到通向不可串行化对象的路径,该方法以OpenJDK的序列化机制为模型,并处理以下情况：
   * - primitives
   * - arrays of primitives
   * - arrays of non-primitive objects
   * - Serializable objects
   * - Externalizable objects
   * - writeReplace
   *
   * It does not yet handle writeObject override, but that shouldn't be too hard to do either.
    * 它还没有处理writeObject覆盖,但是也不应该太难做到,
   */
  private[serializer] def find(obj: Any): List[String] = {
    new SerializationDebugger().visit(obj, List.empty)
  }

  private[serializer] var enableDebugging: Boolean = {
    !AccessController.doPrivileged(new sun.security.action.GetBooleanAction(
      "sun.io.serialization.extendedDebugInfo")).booleanValue()
  }

  private class SerializationDebugger {

    /** A set to track the list of objects we have visited, to avoid cycles in the graph.
      * 一组跟踪我们访问过的对象列表,以避免图中的循环 */
    private val visited = new mutable.HashSet[Any]

    /**
     * Visit the object and its fields and stop when we find an object that is not serializable.
     * Return the path as a list. If everything can be serialized, return an empty list.
      * 访问对象及其字段,并在我们找到不可序列化的对象时停止,将路径作为列表返回,如果一切都可以序列化,返回一个空列表。
     */
    def visit(o: Any, stack: List[String]): List[String] = {
      if (o == null) {
        List.empty
      } else if (visited.contains(o)) {
        List.empty
      } else {
        visited += o
        o match {
          // Primitive value, string, and primitive arrays are always serializable
            //原始值,字符串和原始数组始终是可序列化的
          case _ if o.getClass.isPrimitive => List.empty
          case _: String => List.empty
          case _ if o.getClass.isArray && o.getClass.getComponentType.isPrimitive => List.empty

          // Traverse non primitive array.
            //遍历非原始数组
          case a: Array[_] if o.getClass.isArray && !o.getClass.getComponentType.isPrimitive =>
            val elem = s"array (class ${a.getClass.getName}, size ${a.length})"
            visitArray(o.asInstanceOf[Array[_]], elem :: stack)

          case e: java.io.Externalizable =>
            val elem = s"externalizable object (class ${e.getClass.getName}, $e)"
            visitExternalizable(e, elem :: stack)

          case s: Object with java.io.Serializable =>
            val elem = s"object (class ${s.getClass.getName}, $s)"
            visitSerializable(s, elem :: stack)

          case _ =>
            // Found an object that is not serializable!
            //找到一个不可序列化的对象！
            s"object not serializable (class: ${o.getClass.getName}, value: $o)" :: stack
        }
      }
    }

    private def visitArray(o: Array[_], stack: List[String]): List[String] = {
      var i = 0
      while (i < o.length) {
        val childStack = visit(o(i), s"element of array (index: $i)" :: stack)
        if (childStack.nonEmpty) {
          return childStack
        }
        i += 1
      }
      return List.empty
    }

    /**
     * Visit an externalizable object.访问外部化对象。
     * Since writeExternal() can choose to add arbitrary objects at the time of serialization,
     * the only way to capture all the objects it will serialize is by using a
     * dummy ObjectOutput that collects all the relevant objects for further testing.
      * 由于writeExternal()可以选择在序列化时添加任意对象,捕获其将序列化的所有对象的唯一方法是
      * 使用一个虚拟ObjectOutput来收集所有相关对象进行进一步测试。
     */
    private def visitExternalizable(o: java.io.Externalizable, stack: List[String]): List[String] =
    {
      val fieldList = new ListObjectOutput
      o.writeExternal(fieldList)
      val childObjects = fieldList.outputArray
      var i = 0
      while (i < childObjects.length) {
        val childStack = visit(childObjects(i), "writeExternal data" :: stack)
        if (childStack.nonEmpty) {
          return childStack
        }
        i += 1
      }
      return List.empty
    }

    private def visitSerializable(o: Object, stack: List[String]): List[String] = {
      // An object contains multiple slots in serialization.
      //一个对象在序列化中包含多个插槽
      // Get the slots and visit fields in all of them.
      //获取所有这些插槽和访问字段
      val (finalObj, desc) = findObjectAndDescriptor(o)

      // If the object has been replaced using writeReplace(),
      //如果使用writeReplace（）替换对象
      // then call visit() on it again to test its type again.
      //然后再次调用visit（）来再次测试它的类型。
      if (!finalObj.eq(o)) {
        return visit(finalObj, s"writeReplace data (class: ${finalObj.getClass.getName})" :: stack)
      }

      // Every class is associated with one or more "slots", each slot refers to the parent
      // classes of this class. These slots are used by the ObjectOutputStream
      // serialization code to recursively serialize the fields of an object and
      // its parent classes. For example, if there are the following classes.
      //每个类与一个或多个“插槽”相关联，每个插槽指的是此类的父类,
      // ObjectOutputStream序列化代码使用这些插槽来递归序列化对象及其父类的字段,例如,如果有以下类。
      //
      //     class ParentClass(parentField: Int)
      //     class ChildClass(childField: Int) extends ParentClass(1)
      //
      // Then serializing the an object Obj of type ChildClass requires first serializing the fields
      // of ParentClass (that is, parentField), and then serializing the fields of ChildClass
      // (that is, childField). Correspondingly, there will be two slots related to this object:
      //
      // 1. ParentClass slot, which will be used to serialize parentField of Obj
      // 2. ChildClass slot, which will be used to serialize childField fields of Obj
      //
      // The following code uses the description of each slot to find the fields in the
      // corresponding object to visit.
      //
      val slotDescs = desc.getSlotDescs
      var i = 0
      while (i < slotDescs.length) {
        val slotDesc = slotDescs(i)
        if (slotDesc.hasWriteObjectMethod) {
          // If the class type corresponding to current slot has writeObject() defined,
          // then its not obvious which fields of the class will be serialized as the writeObject()
          // can choose arbitrary fields for serialization. This case is handled separately.
          //如果与当前插槽对应的类型具有writeObject（）定义,
          // 那么该类的哪些字段将被序列化为writeObject（）可以选择任意字段进行序列化。 这种情况分开处理。
          val elem = s"writeObject data (class: ${slotDesc.getName})"
          val childStack = visitSerializableWithWriteObjectMethod(finalObj, elem :: stack)
          if (childStack.nonEmpty) {
            return childStack
          }
        } else {
          // Visit all the fields objects of the class corresponding to the current slot.
          //访问与当前插槽对应的类的所有字段对象
          val fields: Array[ObjectStreamField] = slotDesc.getFields
          val objFieldValues: Array[Object] = new Array[Object](slotDesc.getNumObjFields)
          val numPrims = fields.length - objFieldValues.length
          slotDesc.getObjFieldValues(finalObj, objFieldValues)

          var j = 0
          while (j < objFieldValues.length) {
            val fieldDesc = fields(numPrims + j)
            val elem = s"field (class: ${slotDesc.getName}" +
              s", name: ${fieldDesc.getName}" +
              s", type: ${fieldDesc.getType})"
            val childStack = visit(objFieldValues(j), elem :: stack)
            if (childStack.nonEmpty) {
              return childStack
            }
            j += 1
          }
        }
        i += 1
      }
      return List.empty
    }

    /**
     * Visit a serializable object which has the writeObject() defined.
      * 访问已定义writeObject（）的可序列化对象。
     * Since writeObject() can choose to add arbitrary objects at the time of serialization,
     * the only way to capture all the objects it will serialize is by using a
     * dummy ObjectOutputStream that collects all the relevant fields for further testing.
     * This is similar to how externalizable objects are visited.
     */
    private def visitSerializableWithWriteObjectMethod(
        o: Object, stack: List[String]): List[String] = {
      val innerObjectsCatcher = new ListObjectOutputStream
      var notSerializableFound = false
      try {
        innerObjectsCatcher.writeObject(o)
      } catch {
        case io: IOException =>
          notSerializableFound = true
      }

      // If something was not serializable, then visit the captured objects.
      //如果某些东西不可序列化，那么请访问捕获的对象。
      // Otherwise, all the captured objects are safely serializable, so no need to visit them.
      // As an optimization, just added them to the visited list.
      //否则，所有捕获的对象都可以安全地序列化,所以不需要访问它们,作为优化,只需将它们添加到访问列表。
      if (notSerializableFound) {
        val innerObjects = innerObjectsCatcher.outputArray
        var k = 0
        while (k < innerObjects.length) {
          val childStack = visit(innerObjects(k), stack)
          if (childStack.nonEmpty) {
            return childStack
          }
          k += 1
        }
      } else {
        visited ++= innerObjectsCatcher.outputArray
      }
      return List.empty
    }
  }

  /**
   * Find the object to serialize and the associated [[ObjectStreamClass]]. This method handles
   * writeReplace in Serializable. It starts with the object itself, and keeps calling the
   * writeReplace method until there is no more.
    * 查找要序列化的对象和关联的[[ObjectStreamClass]],此方法处理Serializable中的writeReplace,
    * 它从对象本身开始，并继续调用writeReplace方法,直到没有更多。
   */
  @tailrec
  private def findObjectAndDescriptor(o: Object): (Object, ObjectStreamClass) = {
    val cl = o.getClass
    val desc = ObjectStreamClass.lookupAny(cl)
    if (!desc.hasWriteReplaceMethod) {
      (o, desc)
    } else {
      // write place
      findObjectAndDescriptor(desc.invokeWriteReplace(o))
    }
  }

  /**
   * A dummy [[ObjectOutput]] that simply saves the list of objects written by a writeExternal
   * call, and returns them through `outputArray`.
    * 一个简单地保存由writeExternal写入的对象列表的哑[[ObjectOutput]]调用,并通过`outputArray`返回它们。
   */
  private class ListObjectOutput extends ObjectOutput {
    private val output = new mutable.ArrayBuffer[Any]
    def outputArray: Array[Any] = output.toArray
    override def writeObject(o: Any): Unit = output += o
    override def flush(): Unit = {}
    override def write(i: Int): Unit = {}
    override def write(bytes: Array[Byte]): Unit = {}
    override def write(bytes: Array[Byte], i: Int, i1: Int): Unit = {}
    override def close(): Unit = {}
    override def writeFloat(v: Float): Unit = {}
    override def writeChars(s: String): Unit = {}
    override def writeDouble(v: Double): Unit = {}
    override def writeUTF(s: String): Unit = {}
    override def writeShort(i: Int): Unit = {}
    override def writeInt(i: Int): Unit = {}
    override def writeBoolean(b: Boolean): Unit = {}
    override def writeBytes(s: String): Unit = {}
    override def writeChar(i: Int): Unit = {}
    override def writeLong(l: Long): Unit = {}
    override def writeByte(i: Int): Unit = {}
  }

  /** An output stream that emulates /dev/null */
  private class NullOutputStream extends OutputStream {
    override def write(b: Int) { }
  }

  /**
   * A dummy [[ObjectOutputStream]] that saves the list of objects written to it and returns
   * them through `outputArray`. This works by using the [[ObjectOutputStream]]'s `replaceObject()`
   * method which gets called on every object, only if replacing is enabled. So this subclass
   * of [[ObjectOutputStream]] enabled replacing, and uses replaceObject to get the objects that
   * are being serializabled. The serialized bytes are ignored by sending them to a
   * [[NullOutputStream]], which acts like a /dev/null.
   */
  private class ListObjectOutputStream extends ObjectOutputStream(new NullOutputStream) {
    private val output = new mutable.ArrayBuffer[Any]
    this.enableReplaceObject(true)

    def outputArray: Array[Any] = output.toArray

    override def replaceObject(obj: Object): Object = {
      output += obj
      obj
    }
  }

  /** An implicit class that allows us to call private methods of ObjectStreamClass.
    * 一个隐式类,允许我们调用ObjectStreamClass的私有方法 */
  implicit class ObjectStreamClassMethods(val desc: ObjectStreamClass) extends AnyVal {
    def getSlotDescs: Array[ObjectStreamClass] = {
      reflect.GetClassDataLayout.invoke(desc).asInstanceOf[Array[Object]].map {
        classDataSlot => reflect.DescField.get(classDataSlot).asInstanceOf[ObjectStreamClass]
      }
    }

    def hasWriteObjectMethod: Boolean = {
      reflect.HasWriteObjectMethod.invoke(desc).asInstanceOf[Boolean]
    }

    def hasWriteReplaceMethod: Boolean = {
      reflect.HasWriteReplaceMethod.invoke(desc).asInstanceOf[Boolean]
    }

    def invokeWriteReplace(obj: Object): Object = {
      reflect.InvokeWriteReplace.invoke(desc, obj)
    }

    def getNumObjFields: Int = {
      reflect.GetNumObjFields.invoke(desc).asInstanceOf[Int]
    }

    def getObjFieldValues(obj: Object, out: Array[Object]): Unit = {
      reflect.GetObjFieldValues.invoke(desc, obj, out)
    }
  }

  /**
   * Object to hold all the reflection objects. If we run on a JVM that we cannot understand,
   * this field will be null and this the debug helper should be disabled.
    * 对象来保存所有的反射对象,如果我们运行在我们无法理解的JVM上,此字段将为空,并且应该禁用调试助手。
   */
  private val reflect: ObjectStreamClassReflection = try {
    new ObjectStreamClassReflection
  } catch {
    case e: Exception =>
      logWarning("Cannot find private methods using reflection", e)
      null
  }

  private class ObjectStreamClassReflection {
    /** ObjectStreamClass.getClassDataLayout */
    val GetClassDataLayout: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getClassDataLayout")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.hasWriteObjectMethod */
    val HasWriteObjectMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteObjectMethod")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.hasWriteReplaceMethod */
    val HasWriteReplaceMethod: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("hasWriteReplaceMethod")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.invokeWriteReplace */
    val InvokeWriteReplace: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("invokeWriteReplace", classOf[Object])
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.getNumObjFields */
    val GetNumObjFields: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod("getNumObjFields")
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass.getObjFieldValues */
    val GetObjFieldValues: Method = {
      val f = classOf[ObjectStreamClass].getDeclaredMethod(
        "getObjFieldValues", classOf[Object], classOf[Array[Object]])
      f.setAccessible(true)
      f
    }

    /** ObjectStreamClass$ClassDataSlot.desc field */
    val DescField: Field = {
      // scalastyle:off classforname
      val f = Class.forName("java.io.ObjectStreamClass$ClassDataSlot").getDeclaredField("desc")
      // scalastyle:on classforname
      f.setAccessible(true)
      f
    }
  }
}
