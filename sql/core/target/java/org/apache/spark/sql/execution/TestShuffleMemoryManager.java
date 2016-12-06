package org.apache.spark.sql.execution;
/**
 * A {@link ShuffleMemoryManager} that can be controlled to run out of memory.
 * &#x53ef;&#x4ee5;&#x63a7;&#x5236;&#x8fd0;&#x884c;&#x65f6;&#x7684;&#x5185;&#x5b58;
 */
public  class TestShuffleMemoryManager extends org.apache.spark.shuffle.ShuffleMemoryManager {
  public   TestShuffleMemoryManager () { throw new RuntimeException(); }
  private  boolean oom () { throw new RuntimeException(); }
  public  long tryToAcquire (long numBytes) { throw new RuntimeException(); }
  public  void release (long numBytes) { throw new RuntimeException(); }
  public  void markAsOutOfMemory () { throw new RuntimeException(); }
}
