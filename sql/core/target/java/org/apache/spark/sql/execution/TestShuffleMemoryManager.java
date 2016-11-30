package org.apache.spark.sql.execution;
/**
 * A {@link ShuffleMemoryManager} that can be controlled to run out of memory.
 */
public  class TestShuffleMemoryManager extends org.apache.spark.shuffle.ShuffleMemoryManager {
  public   TestShuffleMemoryManager () { throw new RuntimeException(); }
  private  boolean oom () { throw new RuntimeException(); }
  public  long tryToAcquire (long numBytes) { throw new RuntimeException(); }
  public  void release (long numBytes) { throw new RuntimeException(); }
  public  void markAsOutOfMemory () { throw new RuntimeException(); }
}
