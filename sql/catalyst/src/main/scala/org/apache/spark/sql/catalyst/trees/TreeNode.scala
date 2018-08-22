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

package org.apache.spark.sql.catalyst.trees

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.types.DataType

/** Used by [[TreeNode.getNodeNumbered]] when traversing the tree for a given number
  * 遍历给定数字的树时由[[TreeNode.getNodeNumbered]]使用*/
private class MutableInt(var i: Int)

case class Origin(
  line: Option[Int] = None,
  startPosition: Option[Int] = None)

/**
 * Provides a location for TreeNodes to ask about the context of their origin.  For example, which
 * line of code is currently being parsed.
  * 为TreeNodes提供一个位置,询问其来源的上下文,例如,当前正在解析哪行代码
 */
object CurrentOrigin {
  private val value = new ThreadLocal[Origin]() {
    override def initialValue: Origin = Origin()
  }

  def get: Origin = value.get()
  def set(o: Origin): Unit = value.set(o)

  def reset(): Unit = value.set(Origin())

  def setPosition(line: Int, start: Int): Unit = {
    value.set(
      value.get.copy(line = Some(line), startPosition = Some(start)))
  }

  def withOrigin[A](o: Origin)(f: => A): A = {
    set(o)
    val ret = try f finally { reset() }
    reset()
    ret
  }
}

abstract class TreeNode[BaseType <: TreeNode[BaseType]] extends Product {
  self: BaseType =>

  val origin: Origin = CurrentOrigin.get

  /**
   * Returns a Seq of the children of this node.
    * 返回此节点的子节点的Seq
   * Children should not change. Immutability required for containsChild optimization
    * 子不应该改变,containsChild优化所需的不变性
   */
  def children: Seq[BaseType]

  lazy val containsChild: Set[TreeNode[_]] = children.toSet

  /**
   * Faster version of equality which short-circuits when two treeNodes are the same instance.
   * We don't just override Object.equals, as doing so prevents the scala compiler from
   * generating case class `equals` methods
    * 更快的相等版本,当两个treeNodes是同一个实例时会短路,
    * 我们不只是重写Object.equals,因为这样做会阻止scala编译器生成case类`equals`方法
   */
  def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other) || this == other
  }

  /**
   * Find the first [[TreeNode]] that satisfies the condition specified by `f`.
   * The condition is recursively applied to this node and all of its children (pre-order).
    * 找到满足`f`指定条件的第一个[[TreeNode]],该条件以递归方式应用于该节点及其所有子节点(预订)
   */
  def find(f: BaseType => Boolean): Option[BaseType] = f(this) match {
    case true => Some(this)
    case false => children.foldLeft(None: Option[BaseType]) { (l, r) => l.orElse(r.find(f)) }
  }

  /**
   * Runs the given function on this node and then recursively on [[children]].
    * 在此节点上运行给定函数,然后在[[children]]上递归运行
   * @param f the function to be applied to each node in the tree.
   */
  def foreach(f: BaseType => Unit): Unit = {
    f(this)
    children.foreach(_.foreach(f))
  }

  /**
   * Runs the given function recursively on [[children]] then on this node.
    * 在[[children]]上递归运行给定函数,然后在此节点上运行
   * @param f the function to be applied to each node in the tree.
   */
  def foreachUp(f: BaseType => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  /**
   * Returns a Seq containing the result of applying the given function to each
   * node in this tree in a preorder traversal.
    * 返回一个Seq，其中包含在前序遍历中将给定函数应用于此树中的每个节点的结果
   * @param f the function to be applied.
   */
  def map[A](f: BaseType => A): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret += f(_))
    ret
  }

  /**
   * Returns a Seq by applying a function to all nodes in this tree and using the elements of the
   * resulting collections.
    * 通过将函数应用于此树中的所有节点并使用生成的集合的元素来返回Seq
   */
  def flatMap[A](f: BaseType => TraversableOnce[A]): Seq[A] = {
    val ret = new collection.mutable.ArrayBuffer[A]()
    foreach(ret ++= f(_))
    ret
  }

  /**
   * Returns a Seq containing the result of applying a partial function to all elements in this
   * tree on which the function is defined.
    * 返回一个Seq,其中包含将部分函数应用于此树中定义函数的所有元素的结果。
   */
  def collect[B](pf: PartialFunction[BaseType, B]): Seq[B] = {
    val ret = new collection.mutable.ArrayBuffer[B]()
    val lifted = pf.lift
    foreach(node => lifted(node).foreach(ret.+=))
    ret
  }

  /**
   * Finds and returns the first [[TreeNode]] of the tree for which the given partial function
   * is defined (pre-order), and applies the partial function to it.
    * 查找并返回定义了给定部分函数的树的第一个[[TreeNode]](预订),并将部分函数应用于它
   */
  def collectFirst[B](pf: PartialFunction[BaseType, B]): Option[B] = {
    val lifted = pf.lift
    lifted(this).orElse {
      children.foldLeft(None: Option[B]) { (l, r) => l.orElse(r.collectFirst(pf)) }
    }
  }

  /**
   * Returns a copy of this node where `f` has been applied to all the nodes children.
    * 返回此节点的副本,其中`f`已应用于所有节点子节点
   */
  def mapChildren(f: BaseType => BaseType): BaseType = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = f(arg.asInstanceOf[BaseType])
        if (newChild fastEquals arg) {
          arg
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node with the children replaced.
    * 返回更换子节点的此节点的副本
   * TODO: Validate somewhere (in debug mode?) that children are ordered correctly.
   */
  def withNewChildren(newChildren: Seq[BaseType]): BaseType = {
    assert(newChildren.size == children.size, "Incorrect number of children")
    var changed = false
    val remainingNewChildren = newChildren.toBuffer
    val remainingOldChildren = children.toBuffer
    val newArgs = productIterator.map {
      // Handle Seq[TreeNode] in TreeNode parameters.
      case s: Seq[_] => s.map {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = remainingNewChildren.remove(0)
          val oldChild = remainingOldChildren.remove(0)
          if (newChild fastEquals oldChild) {
            oldChild
          } else {
            changed = true
            newChild
          }
        case nonChild: AnyRef => nonChild
        case null => null
      }
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = remainingNewChildren.remove(0)
        val oldChild = remainingOldChildren.remove(0)
        if (newChild fastEquals oldChild) {
          oldChild
        } else {
          changed = true
          newChild
        }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
    *
    * 返回此节点的副本,其中`rule`已递归地应用于树,当`rule`不适用于给定节点时,它保持不变
    *
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to it and all of its
   * children (pre-order). When `rule` does not apply to a given node it is left unchanged.
    *
    * 返回此节点的副本,其中`rule`已递归地应用于它及其所有子节点(预订),当`rule`不适用于给定节点时,它保持不变。
    *
   * @param rule the function used to transform this nodes children
   */
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    //检查是否未更改,然后可能返回旧副本以避免gc流失
    if (this fastEquals afterRule) {
      transformChildren(rule, (t, r) => t.transformDown(r))
    } else {
      afterRule.transformChildren(rule, (t, r) => t.transformDown(r))
    }
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied to all the children of
   * this node.  When `rule` does not apply to a given node it is left unchanged.
    *
    * 返回此节点的副本,其中`rule`已递归地应用于此节点的所有子节点,当`rule`不适用于给定节点时,它保持不变。
    *
   * @param rule the function used to transform this nodes children
   */
  protected def transformChildren(
      rule: PartialFunction[BaseType, BaseType],
      nextOperation: (BaseType, PartialFunction[BaseType, BaseType]) => BaseType): BaseType = {
    var changed = false
    val newArgs = productIterator.map {
      case arg: TreeNode[_] if containsChild(arg) =>
        val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          newChild
        } else {
          arg
        }
      case Some(arg: TreeNode[_]) if containsChild(arg) =>
        val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
        if (!(newChild fastEquals arg)) {
          changed = true
          Some(newChild)
        } else {
          Some(arg)
        }
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case args: Traversable[_] => args.map {
        case arg: TreeNode[_] if containsChild(arg) =>
          val newChild = nextOperation(arg.asInstanceOf[BaseType], rule)
          if (!(newChild fastEquals arg)) {
            changed = true
            newChild
          } else {
            arg
          }
        case other => other
      }
      case nonChild: AnyRef => nonChild
      case null => null
    }.toArray
    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.
    * 返回此节点的副本,其中`rule`首先递归地应用于其所有子节点,然后自身(post-order),当`rule`不适用于给定节点时,它保持不变。
    *
   * @param rule the function use to transform this nodes children
   */
  def transformUp(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    val afterRuleOnChildren = transformChildren(rule, (t, r) => t.transformUp(r))
    if (this fastEquals afterRuleOnChildren) {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(this, identity[BaseType])
      }
    } else {
      CurrentOrigin.withOrigin(origin) {
        rule.applyOrElse(afterRuleOnChildren, identity[BaseType])
      }
    }
  }

  /**
   * Args to the constructor that should be copied, but not transformed.
   * These are appended to the transformed args automatically by makeCopy
    * 应该复制但不转换的构造函数的Args,这些由makeCopy自动附加到变换的args
    * @return
   */
  protected def otherCopyArgs: Seq[AnyRef] = Nil

  /**
   * Creates a copy of this type of tree node after a transformation.
    * 转换后创建此类树节点的副本
   * Must be overridden by child classes that have constructor arguments
   * that are not present in the productIterator.
    * 必须由具有productIterator中不存在的构造函数参数的子类重写
   * @param newArgs the new product arguments.
   */
  def makeCopy(newArgs: Array[AnyRef]): BaseType = attachTree(this, "makeCopy") {
    val ctors = getClass.getConstructors.filter(_.getParameterTypes.size != 0)
    if (ctors.isEmpty) {
      sys.error(s"No valid constructor for $nodeName")
    }
    val defaultCtor = ctors.maxBy(_.getParameterTypes.size)

    try {
      CurrentOrigin.withOrigin(origin) {
        // Skip no-arg constructors that are just there for kryo.
        if (otherCopyArgs.isEmpty) {
          defaultCtor.newInstance(newArgs: _*).asInstanceOf[BaseType]
        } else {
          defaultCtor.newInstance((newArgs ++ otherCopyArgs).toArray: _*).asInstanceOf[BaseType]
        }
      }
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw new TreeNodeException(
          this,
          s"""
             |Failed to copy node.
             |Is otherCopyArgs specified correctly for $nodeName.
             |Exception message: ${e.getMessage}
             |ctor: $defaultCtor?
             |args: ${newArgs.mkString(", ")}
           """.stripMargin)
    }
  }

  /** Returns the name of this type of TreeNode.  Defaults to the class name.
    * 返回此类TreeNode的名称,默认为类名*/
  def nodeName: String = getClass.getSimpleName

  /**
   * The arguments that should be included in the arg string.  Defaults to the `productIterator`.
    * 应该包含在arg字符串中的参数,默认为`productIterator`
   */
  protected def stringArgs: Iterator[Any] = productIterator

  /** Returns a string representing the arguments to this node, minus any children
    * 返回表示此节点的参数的字符串,减去所有子节点*/
  def argString: String = productIterator.flatMap {
    case tn: TreeNode[_] if containsChild(tn) => Nil
    case tn: TreeNode[_] if tn.toString contains "\n" => s"(${tn.simpleString})" :: Nil
    case seq: Seq[BaseType] if seq.toSet.subsetOf(children.toSet) => Nil
    case seq: Seq[_] => seq.mkString("[", ",", "]") :: Nil
    case set: Set[_] => set.mkString("{", ",", "}") :: Nil
    case other => other :: Nil
  }.mkString(", ")

  /** String representation of this node without any children
    * 此节点的字符串表示,没有任何子节点*/
  def simpleString: String = s"$nodeName $argString".trim

  override def toString: String = treeString

  /** Returns a string representation of the nodes in this tree
    * 返回此树中节点的字符串表示形式*/
  def treeString: String = generateTreeString(0, new StringBuilder).toString

  /**
   * Returns a string representation of the nodes in this tree, where each operator is numbered.
    * 返回此树中节点的字符串表示形式,其中每个运算符都已编号
   * The numbers can be used with [[trees.TreeNode.apply apply]] to easily access specific subtrees.
   */
  def numberedTreeString: String =
    treeString.split("\n").zipWithIndex.map { case (line, i) => f"$i%02d $line" }.mkString("\n")

  /**
   * Returns the tree node at the specified number.
    * 返回指定数字的树节点
   * Numbers for each node can be found in the [[numberedTreeString]].
    * 每个节点的编号可以在[[numbersTreeString]]中找到
   */
  def apply(number: Int): BaseType = getNodeNumbered(new MutableInt(number))

  protected def getNodeNumbered(number: MutableInt): BaseType = {
    if (number.i < 0) {
      null.asInstanceOf[BaseType]
    } else if (number.i == 0) {
      this
    } else {
      number.i -= 1
      children.map(_.getNodeNumbered(number)).find(_ != null).getOrElse(null.asInstanceOf[BaseType])
    }
  }

  /** Appends the string represent of this node and its children to the given StringBuilder.
    * 将此节点及其子节点的字符串表示追加到给定的StringBuilder*/
  protected def generateTreeString(depth: Int, builder: StringBuilder): StringBuilder = {
    builder.append(" " * depth)
    builder.append(simpleString)
    builder.append("\n")
    children.foreach(_.generateTreeString(depth + 1, builder))
    builder
  }

  /**
   * Returns a 'scala code' representation of this `TreeNode` and its children.  Intended for use
   * when debugging where the prettier toString function is obfuscating the actual structure. In the
   * case of 'pure' `TreeNodes` that only contain primitives and other TreeNodes, the result can be
   * pasted in the REPL to build an equivalent Tree.
    * 返回此“TreeNode”及其子节点的“scala代码”表示形式,用于调试时,更漂亮的toString函数混淆了实际结构,
    * 对于仅包含基元和其他TreeNode的“纯”“TreeNodes”,可以将结果粘贴到REPL中以构建等效的Tree
   */
  def asCode: String = {
    val args = productIterator.map {
      case tn: TreeNode[_] => tn.asCode
      case s: String => "\"" + s + "\""
      case other => other.toString
    }
    s"$nodeName(${args.mkString(",")})"
  }
}
