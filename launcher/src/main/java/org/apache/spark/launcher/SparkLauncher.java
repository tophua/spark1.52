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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Launcher for Spark applications.
 * Spark应用程序的启动器
 * <p>
 * Use this class to start Spark applications programmatically. The class uses a builder pattern
 * to allow clients to configure the Spark application and launch it as a child process.
 * 使用此类以编程方式启动Spark应用程序,该类使用构建器模式来允许客户端配置Spark应用程序并将其作为子进程启动
 * </p>
 */
public class SparkLauncher {

  /** The Spark master. */
  public static final String SPARK_MASTER = "spark.master";

  /** Configuration key for the driver memory.
   * 驱动程序内存的配置键*/
  public static final String DRIVER_MEMORY = "spark.driver.memory";
  /** Configuration key for the driver class path.
   * 驱动程序类路径的配置键*/
  public static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
  /** Configuration key for the driver VM options.
   * 驱动程序VM选项的配置键*/
  public static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
  /** Configuration key for the driver native library path.
   * 驱动程序本机库路径的配置键*/
  public static final String DRIVER_EXTRA_LIBRARY_PATH = "spark.driver.extraLibraryPath";

  /** Configuration key for the executor memory.
   * 执行器内存的配置键 */
  public static final String EXECUTOR_MEMORY = "spark.executor.memory";
  /** Configuration key for the executor class path.
   * 执行器类路径的配置键 */
  public static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";
  /** Configuration key for the executor VM options.
   * 执行器VM选项的配置键*/
  public static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
  /** Configuration key for the executor native library path.
   * 执行器本机库路径的配置键。 */
  public static final String EXECUTOR_EXTRA_LIBRARY_PATH = "spark.executor.extraLibraryPath";
  /** Configuration key for the number of executor CPU cores.
   * 执行器CPU核心数量的配置键*/
  public static final String EXECUTOR_CORES = "spark.executor.cores";

  // Visible for testing. 可见测试
  final SparkSubmitCommandBuilder builder;

  public SparkLauncher() {
    this(null);
  }

  /**
   * Creates a launcher that will set the given environment variables in the child.
   * 创建一个启动器,它将在孩子中设置给定的环境变量
   *
   * @param env Environment variables to set.
   */
  public SparkLauncher(Map<String, String> env) {
    this.builder = new SparkSubmitCommandBuilder();
    if (env != null) {
      this.builder.childEnv.putAll(env);
    }
  }

  /**
   * Set a custom JAVA_HOME for launching the Spark application.
   * 设置一个用于启动Spark应用程序的自定义JAVA_HOME
   *
   * @param javaHome Path to the JAVA_HOME to use.
   * @return This launcher.
   */
  public SparkLauncher setJavaHome(String javaHome) {
    checkNotNull(javaHome, "javaHome");
    builder.javaHome = javaHome;
    return this;
  }

  /**
   * Set a custom Spark installation location for the application.
   * 为应用程序设置自定义Spark安装位置
   *
   * @param sparkHome Path to the Spark installation to use.
   * @return This launcher.
   */
  public SparkLauncher setSparkHome(String sparkHome) {
    checkNotNull(sparkHome, "sparkHome");
    builder.childEnv.put(ENV_SPARK_HOME, sparkHome);
    return this;
  }

  /**
   * Set a custom properties file with Spark configuration for the application.
   * 为应用程序设置Spark配置的自定义属性文件
   *
   * @param path Path to custom properties file to use.
   * @return This launcher.
   */
  public SparkLauncher setPropertiesFile(String path) {
    checkNotNull(path, "path");
    builder.propertiesFile = path;
    return this;
  }

  /**
   * Set a single configuration value for the application.
   * 为应用程序设置单个配置值
   *
   * @param key Configuration key.
   * @param value The value to use.
   * @return This launcher.
   */
  public SparkLauncher setConf(String key, String value) {
    checkNotNull(key, "key");
    checkNotNull(value, "value");
    checkArgument(key.startsWith("spark."), "'key' must start with 'spark.'");
    builder.conf.put(key, value);
    return this;
  }

  /**
   * Set the application name.
   *设置应用程序名称
   * @param appName Application name.
   * @return This launcher.
   */
  public SparkLauncher setAppName(String appName) {
    checkNotNull(appName, "appName");
    builder.appName = appName;
    return this;
  }

  /**
   * Set the Spark master for the application.
   * 设置应用程序的Spark master
   *
   * @param master Spark master.
   * @return This launcher.
   */
  public SparkLauncher setMaster(String master) {
    checkNotNull(master, "master");
    builder.master = master;
    return this;
  }

  /**
   * Set the deploy mode for the application.
   * 设置应用程序的部署模式
   * @param mode Deploy mode.
   * @return This launcher.
   */
  public SparkLauncher setDeployMode(String mode) {
    checkNotNull(mode, "mode");
    builder.deployMode = mode;
    return this;
  }

  /**
   * Set the main application resource. This should be the location of a jar file for Scala/Java
   * applications, or a python script for PySpark applications.
   * 设置主应用资源,这应该是Scala / Java应用程序的jar文件的位置，或PySpark应用程序的python脚本。
   * @param resource Path to the main application resource.
   * @return This launcher.
   */
  public SparkLauncher setAppResource(String resource) {
    checkNotNull(resource, "resource");
    builder.appResource = resource;
    return this;
  }

  /**
   * Sets the application class name for Java/Scala applications.
   * 设置Java / Scala应用程序的应用程序类名称。
   * @param mainClass Application's main class.
   * @return This launcher.
   */
  public SparkLauncher setMainClass(String mainClass) {
    checkNotNull(mainClass, "mainClass");
    builder.mainClass = mainClass;
    return this;
  }

  /**
   * Adds a no-value argument to the Spark invocation. If the argument is known, this method
   * validates whether the argument is indeed a no-value argument, and throws an exception
   * otherwise.
   * 向Spark调用添加无值参数,如果参数已知,则此方法验证参数是否确实是无值参数,否则引发异常,
   * <p>
   * Use this method with caution. It is possible to create an invalid Spark command by passing
   * unknown arguments to this method, since those are allowed for forward compatibility.
   *
   * 请谨慎使用此方法,可以通过将未知参数传递给此方法来创建无效的Spark命令,因为这些参数被允许向前兼容
   *
   * @param arg Argument to add.
   * @return This launcher.
   */
  public SparkLauncher addSparkArg(String arg) {
    SparkSubmitOptionParser validator = new ArgumentValidator(false);
    validator.parse(Arrays.asList(arg));
    builder.sparkArgs.add(arg);
    return this;
  }

  /**
   * Adds an argument with a value to the Spark invocation. If the argument name corresponds to
   * a known argument, the code validates that the argument actually expects a value, and throws
   * an exception otherwise.
   * 向Spark调用添加一个带值的参数,如果参数名称对应于已知参数,则代码将验证参数实际预期值,否则抛出异常
   * <p>
   * It is safe to add arguments modified by other methods in this class (such as
   * {@link #setMaster(String)} - the last invocation will be the one to take effect.
   * <p>
   * Use this method with caution. It is possible to create an invalid Spark command by passing
   * unknown arguments to this method, since those are allowed for forward compatibility.
   * 请谨慎使用此方法,可以通过将未知参数传递给此方法来创建无效的Spark命令,因为这些参数被允许向前兼容
   *
   * @param name Name of argument to add.
   * @param value Value of the argument.
   * @return This launcher.
   */
  public SparkLauncher addSparkArg(String name, String value) {
    SparkSubmitOptionParser validator = new ArgumentValidator(true);
    if (validator.MASTER.equals(name)) {
      setMaster(value);
    } else if (validator.PROPERTIES_FILE.equals(name)) {
      setPropertiesFile(value);
    } else if (validator.CONF.equals(name)) {
      String[] vals = value.split("=", 2);
      setConf(vals[0], vals[1]);
    } else if (validator.CLASS.equals(name)) {
      setMainClass(value);
    } else if (validator.JARS.equals(name)) {
      builder.jars.clear();
      for (String jar : value.split(",")) {
        addJar(jar);
      }
    } else if (validator.FILES.equals(name)) {
      builder.files.clear();
      for (String file : value.split(",")) {
        addFile(file);
      }
    } else if (validator.PY_FILES.equals(name)) {
      builder.pyFiles.clear();
      for (String file : value.split(",")) {
        addPyFile(file);
      }
    } else {
      validator.parse(Arrays.asList(name, value));
      builder.sparkArgs.add(name);
      builder.sparkArgs.add(value);
    }
    return this;
  }

  /**
   * Adds command line arguments for the application.
   * 为应用程序添加命令行参数
   *
   * @param args Arguments to pass to the application's main class.
   * @return This launcher.
   */
  public SparkLauncher addAppArgs(String... args) {
    for (String arg : args) {
      checkNotNull(arg, "arg");
      builder.appArgs.add(arg);
    }
    return this;
  }

  /**
   * Adds a jar file to be submitted with the application.
   * 添加一个应用程序提交的jar文件
   *
   * @param jar Path to the jar file.
   * @return This launcher.
   */
  public SparkLauncher addJar(String jar) {
    checkNotNull(jar, "jar");
    builder.jars.add(jar);
    return this;
  }

  /**
   * Adds a file to be submitted with the application.
   * 添加要与应用程序一起提交的文件
   *
   * @param file Path to the file.
   * @return This launcher.
   */
  public SparkLauncher addFile(String file) {
    checkNotNull(file, "file");
    builder.files.add(file);
    return this;
  }

  /**
   * Adds a python file / zip / egg to be submitted with the application.
   * 添加与应用程序一起提交的python文件/ zip / egg
   *
   * @param file Path to the file.
   * @return This launcher.
   */
  public SparkLauncher addPyFile(String file) {
    checkNotNull(file, "file");
    builder.pyFiles.add(file);
    return this;
  }

  /**
   * Enables verbose reporting for SparkSubmit.
   * 启用SparkSubmit的详细报告
   *
   * @param verbose Whether to enable verbose output.
   * @return This launcher.
   */
  public SparkLauncher setVerbose(boolean verbose) {
    builder.verbose = verbose;
    return this;
  }

  /**
   * Launches a sub-process that will start the configured Spark application.
   * 启动一个子进程,启动配置的Spark应用程序
   *
   * @return A process handle for the Spark app.
   */
  public Process launch() throws IOException {
    List<String> cmd = new ArrayList<String>();
    String script = isWindows() ? "spark-submit.cmd" : "spark-submit";
    cmd.add(join(File.separator, builder.getSparkHome(), "bin", script));
    cmd.addAll(builder.buildSparkSubmitArgs());

    // Since the child process is a batch script, let's quote things so that special characters are
    // preserved, otherwise the batch interpreter will mess up the arguments. Batch scripts are
    // weird.
      //由于子进程是一个批处理脚本,我们来引用一些特殊的字符保留,否则批量解释器会弄乱参数,批处理脚本很奇怪。
    if (isWindows()) {
      List<String> winCmd = new ArrayList<String>();
      for (String arg : cmd) {
        winCmd.add(quoteForBatchScript(arg));
      }
      cmd = winCmd;
    }

    ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[cmd.size()]));
    for (Map.Entry<String, String> e : builder.childEnv.entrySet()) {
      pb.environment().put(e.getKey(), e.getValue());
    }
    return pb.start();
  }

  private static class ArgumentValidator extends SparkSubmitOptionParser {

    private final boolean hasValue;

    ArgumentValidator(boolean hasValue) {
      this.hasValue = hasValue;
    }

    @Override
    protected boolean handle(String opt, String value) {
      if (value == null && hasValue) {
        throw new IllegalArgumentException(String.format("'%s' does not expect a value.", opt));
      }
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      // Do not fail on unknown arguments, to support future arguments added to SparkSubmit.
        //不要在未知的参数上失败,以支持添加到SparkSubmit的未来参数
      return true;
    }

    protected void handleExtraArgs(List<String> extra) {
      // No op.
    }

  };

}
