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

package org.apache.spark.graphx;

import java.io.Serializable;

/**
 * Represents a subset of the fields of an [[EdgeTriplet]] or [[EdgeContext]]. This allows the
 * system to populate only those fields for efficiency.
 * 表示[[EdgeTriplet]]或[[EdgeContext]]的字段的子集,这允许系统仅填充那些字段以提高效率
 */
public class TripletFields implements Serializable {

  /** Indicates whether the source vertex attribute is included.
   * 指示是否包含源顶点属性*/
  public final boolean useSrc;

  /** Indicates whether the destination vertex attribute is included.
   * 指示是否包含目标顶点属性 */
  public final boolean useDst;

  /** Indicates whether the edge attribute is included.
   * 指示是否包含edge属性*/
  public final boolean useEdge;

  /** Constructs a default TripletFields in which all fields are included.
   * 构造一个默认的TripletFields，其中包含所有字段*/
  public TripletFields() {
    this(true, true, true);
  }

  public TripletFields(boolean useSrc, boolean useDst, boolean useEdge) {
    this.useSrc = useSrc;
    this.useDst = useDst;
    this.useEdge = useEdge;
  }

  /**
   * None of the triplet fields are exposed.
   * 三个字段都没有暴露
   */
  public static final TripletFields None = new TripletFields(false, false, false);

  /**
   * Expose only the edge field and not the source or destination field.
   * 仅显示边缘字段,而不显示源或目标字段
   */
  public static final TripletFields EdgeOnly = new TripletFields(false, false, true);

  /**
   * Expose the source and edge fields but not the destination field. (Same as Src)
   * 公开源和边缘字段,但不显示目标字段(与Src相同)
   */
  public static final TripletFields Src = new TripletFields(true, false, true);

  /**
   * Expose the destination and edge fields but not the source field. (Same as Dst)
   * 公开目标和边缘字段，但不显示源字段,(与Dst相同）
   */
  public static final TripletFields Dst = new TripletFields(false, true, true);

  /**
   * Expose all the fields (source, edge, and destination).
   * 公开所有字段(源,边缘和目标)
   */
  public static final TripletFields All = new TripletFields(true, true, true);
}
