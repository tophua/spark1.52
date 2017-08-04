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
import java.util.*;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Special command builder for handling a CLI invocation of SparkSubmit.
 * 用于处理SparkSubmit的CLI调用的特殊命令构建器
 * <p>
 * This builder adds command line parsing compatible with SparkSubmit. It handles setting
 * driver-side options and special parsing behavior needed for the special-casing certain internal
 * Spark applications.
 * 此构建器添加与SparkSubmit兼容的命令行解析,它处理设置驱动程序选项和特殊解析行为需要特殊内置的某些内部Spark应用程序。
 * <p>
 * This class has also some special features to aid launching pyspark.
 */
class SparkSubmitCommandBuilder extends AbstractCommandBuilder {

  /**
   * Name of the app resource used to identify the PySpark shell. The command line parser expects
   * the resource name to be the very first argument to spark-submit in this case.
   * 用于标识PySpark shell的应用程序资源的名称。在这种情况下,命令行解析器期望资源名称是spark-submit的第一个参数
   *
   * NOTE: this cannot be "pyspark-shell" since that identifies the PySpark shell to SparkSubmit
   * (see java_gateway.py), and can cause this code to enter into an infinite loop.
   */
  static final String PYSPARK_SHELL = "pyspark-shell-main";

  /**
   * This is the actual resource name that identifies the PySpark shell to SparkSubmit.
   * 这是标识SparkSubmit的PySpark shell的实际资源名称
   */
  static final String PYSPARK_SHELL_RESOURCE = "pyspark-shell";

  /**
   * Name of the app resource used to identify the SparkR shell. The command line parser expects
   * the resource name to be the very first argument to spark-submit in this case.
   * 用于标识SparkR shell的应用程序资源的名称,在这种情况下,命令行解析器期望资源名称是spark-submit的第一个参数
   *
   * NOTE: this cannot be "sparkr-shell" since that identifies the SparkR shell to SparkSubmit
   * (see sparkR.R), and can cause this code to enter into an infinite loop.
   * 注意：这不能是“sparkr-shell”,因为它将SparkR shell标识为SparkSubmit(参见sparkR.R),并且可能导致此代码进入无限循环
   */
  static final String SPARKR_SHELL = "sparkr-shell-main";

  /**
   * This is the actual resource name that identifies the SparkR shell to SparkSubmit.
   * 这是标识SparkSubmit的SparkR shell的实际资源名称
   */
  static final String SPARKR_SHELL_RESOURCE = "sparkr-shell";

  /**
   * This map must match the class names for available special classes, since this modifies the way
   * command line parsing works. This maps the class name to the resource to use when calling
   * spark-submit.
   * 此映射必须与可用的特殊类的类名称相匹配,因为这将修改命令行解析的工作原理,
   * 这会将类名映射到调用spark-submit时使用的资源。
   */
  private static final Map<String, String> specialClasses = new HashMap<String, String>();
  static {
    specialClasses.put("org.apache.spark.repl.Main", "spark-shell");
    specialClasses.put("org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver",
      "spark-internal");
    specialClasses.put("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
      "spark-internal");
  }

  final List<String> sparkArgs;
  private final boolean printHelp;

  /**
   * Controls whether mixing spark-submit arguments with app arguments is allowed. This is needed
   * to parse the command lines for things like bin/spark-shell, which allows users to mix and
   * match arguments (e.g. "bin/spark-shell SparkShellArg --master foo").
   * 控制是否允许使用spark参数混合spark-submit参数,需要解析诸如bin / spark-shell之类的命令行,
   * 这允许用户混合和匹配参数(例如“bin / spark-shell SparkShellArg --master foo”)
   */
  private boolean allowsMixedArguments;

  SparkSubmitCommandBuilder() {
    this.sparkArgs = new ArrayList<String>();
    this.printHelp = false;
  }

  SparkSubmitCommandBuilder(List<String> args) {
    this.sparkArgs = new ArrayList<String>();
    List<String> submitArgs = args;
    if (args.size() > 0 && args.get(0).equals(PYSPARK_SHELL)) {
      this.allowsMixedArguments = true;
      appResource = PYSPARK_SHELL_RESOURCE;
      submitArgs = args.subList(1, args.size());
    } else if (args.size() > 0 && args.get(0).equals(SPARKR_SHELL)) {
      this.allowsMixedArguments = true;
      appResource = SPARKR_SHELL_RESOURCE;
      submitArgs = args.subList(1, args.size());
    } else {
      this.allowsMixedArguments = false;
    }

    OptionParser parser = new OptionParser();
    parser.parse(submitArgs);
    this.printHelp = parser.helpRequested;
  }

  @Override
  public List<String> buildCommand(Map<String, String> env) throws IOException {
    if (PYSPARK_SHELL_RESOURCE.equals(appResource) && !printHelp) {
      return buildPySparkShellCommand(env);
    } else if (SPARKR_SHELL_RESOURCE.equals(appResource) && !printHelp) {
      return buildSparkRCommand(env);
    } else {
      return buildSparkSubmitCommand(env);
    }
  }

  List<String> buildSparkSubmitArgs() {
    List<String> args = new ArrayList<String>();
    SparkSubmitOptionParser parser = new SparkSubmitOptionParser();

    if (verbose) {
      args.add(parser.VERBOSE);
    }

    if (master != null) {
      args.add(parser.MASTER);
      args.add(master);
    }

    if (deployMode != null) {
      args.add(parser.DEPLOY_MODE);
      args.add(deployMode);
    }

    if (appName != null) {
      args.add(parser.NAME);
      args.add(appName);
    }

    for (Map.Entry<String, String> e : conf.entrySet()) {
      args.add(parser.CONF);
      args.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }

    if (propertiesFile != null) {
      args.add(parser.PROPERTIES_FILE);
      args.add(propertiesFile);
    }

    if (!jars.isEmpty()) {
      args.add(parser.JARS);
      args.add(join(",", jars));
    }

    if (!files.isEmpty()) {
      args.add(parser.FILES);
      args.add(join(",", files));
    }

    if (!pyFiles.isEmpty()) {
      args.add(parser.PY_FILES);
      args.add(join(",", pyFiles));
    }

    if (mainClass != null) {
      args.add(parser.CLASS);
      args.add(mainClass);
    }

    args.addAll(sparkArgs);
    if (appResource != null) {
      args.add(appResource);
    }
    args.addAll(appArgs);

    return args;
  }

  private List<String> buildSparkSubmitCommand(Map<String, String> env) throws IOException {
    // Load the properties file and check whether spark-submit will be running the app's driver
    // or just launching a cluster app. When running the driver, the JVM's argument will be
    // modified to cover the driver's configuration.
      //加载属性文件/并检查spark-submit是否将运行应用程序的驱动程序
      //或者只是启动一个集群应用程序/运行驱动程序时/JVM的参数将被修改以覆盖驱动程序的配置。
    Properties props = loadPropertiesFile();
    boolean isClientMode = isClientMode(props);
    String extraClassPath = isClientMode ?
      firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_CLASSPATH, conf, props) : null;

    List<String> cmd = buildJavaCommand(extraClassPath);
    // Take Thrift Server as daemon
      //以Thrift Server为守护进程
    if (isThriftServer(mainClass)) {
      addOptionString(cmd, System.getenv("SPARK_DAEMON_JAVA_OPTS"));
    }
    addOptionString(cmd, System.getenv("SPARK_SUBMIT_OPTS"));
    addOptionString(cmd, System.getenv("SPARK_JAVA_OPTS"));

    if (isClientMode) {
      // Figuring out where the memory value come from is a little tricky due to precedence.
      // Precedence is observed in the following order:
      // - explicit configuration (setConf()), which also covers --driver-memory cli argument.
      // - properties file.
      // - SPARK_DRIVER_MEMORY env variable
      // - SPARK_MEM env variable
      // - default value (1g)
      // Take Thrift Server as daemon
        //计算出内存值来自哪里是有点棘手，因为优先级。
        //按照以下顺序观察优先级：
        // - 显式配置（setConf（）），其中还包括--driver-memory cli参数。
        // - 属性文件。
        // - SPARK_DRIVER_MEMORY env变量
        // - SPARK_MEM env变量
        // - 默认值（1g）
        //将Thrift Server作为守护进程
      String tsMemory =
        isThriftServer(mainClass) ? System.getenv("SPARK_DAEMON_MEMORY") : null;
      String memory = firstNonEmpty(tsMemory,
        firstNonEmptyValue(SparkLauncher.DRIVER_MEMORY, conf, props),
        System.getenv("SPARK_DRIVER_MEMORY"), System.getenv("SPARK_MEM"), DEFAULT_MEM);
      cmd.add("-Xms" + memory);
      cmd.add("-Xmx" + memory);
      addOptionString(cmd, firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, conf, props));
      mergeEnvPathList(env, getLibPathEnvName(),
        firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, conf, props));
    }

    addPermGenSizeOpt(cmd);
    cmd.add("org.apache.spark.deploy.SparkSubmit");
    cmd.addAll(buildSparkSubmitArgs());
    return cmd;
  }

  private List<String> buildPySparkShellCommand(Map<String, String> env) throws IOException {
    // For backwards compatibility, if a script is specified in
    // the pyspark command line, then run it using spark-submit.
      //为了向后兼容,如果在pyspark命令行中指定了脚本,则使用spark-submit运行脚本
    if (!appArgs.isEmpty() && appArgs.get(0).endsWith(".py")) {
      System.err.println(
        "WARNING: Running python applications through 'pyspark' is deprecated as of Spark 1.0.\n" +
        "Use ./bin/spark-submit <python file>");
      appResource = appArgs.get(0);
      appArgs.remove(0);
      return buildCommand(env);
    }

    checkArgument(appArgs.isEmpty(), "pyspark does not support any application options.");

    // When launching the pyspark shell, the spark-submit arguments should be stored in the
    // PYSPARK_SUBMIT_ARGS env variable.
      //当启动pyspark shell时，spark-submit参数应该存储在PYSPARK_SUBMIT_ARGS env变量。
    constructEnvVarArgs(env, "PYSPARK_SUBMIT_ARGS");

    // The executable is the PYSPARK_DRIVER_PYTHON env variable set by the pyspark script,
    // followed by PYSPARK_DRIVER_PYTHON_OPTS.
      //可执行文件是pyspark脚本设置的PYSPARK_DRIVER_PYTHON env变量,后跟PYSPARK_DRIVER_PYTHON_OPTS。
    List<String> pyargs = new ArrayList<String>();
    pyargs.add(firstNonEmpty(System.getenv("PYSPARK_DRIVER_PYTHON"), "python"));
    String pyOpts = System.getenv("PYSPARK_DRIVER_PYTHON_OPTS");
    if (!isEmpty(pyOpts)) {
      pyargs.addAll(parseOptionString(pyOpts));
    }

    return pyargs;
  }

  private List<String> buildSparkRCommand(Map<String, String> env) throws IOException {
    if (!appArgs.isEmpty() && appArgs.get(0).endsWith(".R")) {
      appResource = appArgs.get(0);
      appArgs.remove(0);
      return buildCommand(env);
    }
    // When launching the SparkR shell, store the spark-submit arguments in the SPARKR_SUBMIT_ARGS
    // env variable.
      //启动SparkR shell时,将spark-submit参数存储在SPARKR_SUBMIT_ARGS env变量中。
    constructEnvVarArgs(env, "SPARKR_SUBMIT_ARGS");

    // Set shell.R as R_PROFILE_USER to load the SparkR package when the shell comes up.
      //将shell.R设置为R_PROFILE_USER以在Shell启动时加载SparkR包
    String sparkHome = System.getenv("SPARK_HOME");
    env.put("R_PROFILE_USER",
            join(File.separator, sparkHome, "R", "lib", "SparkR", "profile", "shell.R"));

    List<String> args = new ArrayList<String>();
    args.add(firstNonEmpty(System.getenv("SPARKR_DRIVER_R"), "R"));
    return args;
  }

  private void constructEnvVarArgs(
      Map<String, String> env,
      String submitArgsEnvVariable) throws IOException {
    Properties props = loadPropertiesFile();
    mergeEnvPathList(env, getLibPathEnvName(),
      firstNonEmptyValue(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, conf, props));

    StringBuilder submitArgs = new StringBuilder();
    for (String arg : buildSparkSubmitArgs()) {
      if (submitArgs.length() > 0) {
        submitArgs.append(" ");
      }
      submitArgs.append(quoteForCommandString(arg));
    }
    env.put(submitArgsEnvVariable, submitArgs.toString());
  }


  private boolean isClientMode(Properties userProps) {
    String userMaster = firstNonEmpty(master, (String) userProps.get(SparkLauncher.SPARK_MASTER));
    // Default master is "local[*]", so assume client mode in that case.
    return userMaster == null ||
      "client".equals(deployMode) ||
      (!userMaster.equals("yarn-cluster") && deployMode == null);
  }

  /**
   * Return whether the given main class represents a thrift server.
   * 返回给定的主类是否代表thrift服务器。
   */
  private boolean isThriftServer(String mainClass) {
    return (mainClass != null &&
      mainClass.equals("org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"));
  }


  private class OptionParser extends SparkSubmitOptionParser {

    boolean helpRequested = false;

    @Override
    protected boolean handle(String opt, String value) {
      if (opt.equals(MASTER)) {
        master = value;
      } else if (opt.equals(DEPLOY_MODE)) {
        deployMode = value;
      } else if (opt.equals(PROPERTIES_FILE)) {
        propertiesFile = value;
      } else if (opt.equals(DRIVER_MEMORY)) {
        conf.put(SparkLauncher.DRIVER_MEMORY, value);
      } else if (opt.equals(DRIVER_JAVA_OPTIONS)) {
        conf.put(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, value);
      } else if (opt.equals(DRIVER_LIBRARY_PATH)) {
        conf.put(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, value);
      } else if (opt.equals(DRIVER_CLASS_PATH)) {
        conf.put(SparkLauncher.DRIVER_EXTRA_CLASSPATH, value);
      } else if (opt.equals(CONF)) {
        String[] setConf = value.split("=", 2);
        checkArgument(setConf.length == 2, "Invalid argument to %s: %s", CONF, value);
        conf.put(setConf[0], setConf[1]);
      } else if (opt.equals(CLASS)) {
        // The special classes require some special command line handling, since they allow
        // mixing spark-submit arguments with arguments that should be propagated to the shell
        // itself. Note that for this to work, the "--class" argument must come before any
        // non-spark-submit arguments.
          //特殊类需要一些特殊的命令行处理，因为它们允许
          //将spark-submit参数与应该传播到shell的参数进行混合
          //本身 注意，为了这个工作，“--class”参数必须在任何之前
          //非spark-submit参数。
        mainClass = value;
        if (specialClasses.containsKey(value)) {
          allowsMixedArguments = true;
          appResource = specialClasses.get(value);
        }
      } else if (opt.equals(HELP) || opt.equals(USAGE_ERROR)) {
        helpRequested = true;
        sparkArgs.add(opt);
      } else {
        sparkArgs.add(opt);
        if (value != null) {
          sparkArgs.add(value);
        }
      }
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      // When mixing arguments, add unrecognized parameters directly to the user arguments list. In
      // normal mode, any unrecognized parameter triggers the end of command line parsing, and the
      // parameter itself will be interpreted by SparkSubmit as the application resource. The
      // remaining params will be appended to the list of SparkSubmit arguments.
        //混合参数时，将无法识别的参数直接添加到用户参数列表中。 在
        //正常模式下，任何无法识别的参数都会触发命令行解析的结束
        //参数本身将被SparkSubmit解释为应用程序资源。该
        //剩余参数将附加到SparkSubmit参数列表中。
      if (allowsMixedArguments) {
        appArgs.add(opt);
        return true;
      } else {
        checkArgument(!opt.startsWith("-"), "Unrecognized option: %s", opt);
        sparkArgs.add(opt);
        return false;
      }
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {
      for (String arg : extra) {
        sparkArgs.add(arg);
      }
    }

  }

}
