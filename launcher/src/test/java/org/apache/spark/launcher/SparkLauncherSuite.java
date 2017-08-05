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

package org.apache.spark.launcher;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 * These tests require the Spark assembly to be built before they can be run.
 * 这些测试要求Spark组件在运行之前进行构建
 */
public class SparkLauncherSuite {

  private static final Logger LOG = LoggerFactory.getLogger(SparkLauncherSuite.class);
    String spark_test_home="/software/spark152";
  @Test
  //测试Spark 参数处理
  public void testSparkArgumentHandling() throws Exception {


      System.out.println("========"+spark_test_home);
    SparkLauncher launcher = new SparkLauncher()
      .setSparkHome(spark_test_home);
    SparkSubmitOptionParser opts = new SparkSubmitOptionParser();

    launcher.addSparkArg(opts.HELP);
    try {
      launcher.addSparkArg(opts.PROXY_USER);
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    launcher.addSparkArg(opts.PROXY_USER, "someUser");
    try {
      launcher.addSparkArg(opts.HELP, "someValue");
      fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // Expected.
    }

    launcher.addSparkArg("--future-argument");
    launcher.addSparkArg("--future-argument", "someValue");

    launcher.addSparkArg(opts.MASTER, "myMaster");
    assertEquals("myMaster", launcher.builder.master);
    //添加没有效果
    launcher.addJar("foo");
    launcher.addSparkArg(opts.JARS, "bar");
    assertEquals(Arrays.asList("bar"), launcher.builder.jars);

    launcher.addFile("foo");
    launcher.addSparkArg(opts.FILES, "bar");
    assertEquals(Arrays.asList("bar"), launcher.builder.files);

    launcher.addPyFile("foo");
    launcher.addSparkArg(opts.PY_FILES, "bar");
    assertEquals(Arrays.asList("bar"), launcher.builder.pyFiles);

    launcher.setConf("spark.foo", "foo");
    launcher.addSparkArg(opts.CONF, "spark.foo=bar");
    assertEquals("bar", launcher.builder.conf.get("spark.foo"));
  }

  @Test
  //测试子进程启动程序
  public void testChildProcLauncher() throws Exception {
    SparkSubmitOptionParser opts = new SparkSubmitOptionParser();

    Map<String, String> env = new HashMap<String, String>();
    //设置环境变量
    env.put("SPARK_PRINT_LAUNCH_COMMAND", "1");
      //System.getProperty("spark.test.home")
    SparkLauncher launcher = new SparkLauncher(env)
      .setSparkHome(spark_test_home)
      .setMaster("local")
      .setAppResource("spark-internal")
            //--conf, spark.driver.extraJavaOptions=-Dfoo=bar -Dtest.name=-testChildProcLauncher,
      .addSparkArg(opts.CONF,
        String.format("%s=-Dfoo=ShouldBeOverriddenBelow", SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS))
      .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS,
        "-Dfoo=bar -Dtest.name=-testChildProcLauncher")
            //java.class.path  Java 类路径
      .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, System.getProperty("java.class.path"))
            //--class ShouldBeOverriddenBelow 好像没用效果,参数列表没有打印处来
      .addSparkArg(opts.CLASS, "ShouldBeOverriddenBelow")
      .setMainClass(SparkLauncherTestApp.class.getName())
      .addAppArgs("proc");
      System.out.println("opts.CONF:"+opts.CONF);
      System.out.println("extraJavaOptions:"+SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS);
      System.out.println("DRIVER_EXTRA_JAVA_OPTIONS:"+SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS);
      System.out.println("launcher:"+launcher.toString());

    final Process app = launcher.launch();
   new Redirector("stdout", app.getInputStream()).start();
  new Redirector("stderr", app.getErrorStream()).start();
  assertEquals(0, app.waitFor());
  }

  public static class SparkLauncherTestApp {

    public static void main(String[] args) throws Exception {
      assertEquals(1, args.length);
      assertEquals("proc", args[0]);
      assertEquals("bar", System.getProperty("foo"));
      assertEquals("local", System.getProperty(SparkLauncher.SPARK_MASTER));
    }

  }

  private static class Redirector extends Thread {

    private final InputStream in;

    Redirector(String name, InputStream in) {
      this.in = in;
      setName(name);
      System.out.println(name);
      setDaemon(true);
    }

    @Override
    public void run() {
      try {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println("==="+line);
          LOG.warn(line);
        }
      } catch (Exception e) {
        LOG.error("Error reading process output.", e);
      }
    }

  }

}
