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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Command line interface for the Spark launcher. Used internally by Spark scripts.
 * Spark启动器的命令行界面,由Spark脚本内部使用
 */
class Main {

  /**
   * Usage: Main [class] [class args]
   * <p>
   * This CLI works in two different modes:
   * 此CLI可在两种不同的模式下工作：
   * <ul>
   *   <li>"spark-submit": if <i>class</i> is "org.apache.spark.deploy.SparkSubmit", the
   *   {@link SparkLauncher} class is used to launch a Spark application.</li>
   *   如果<i> class </ i>是“org.apache.spark.deploy.SparkSubmit”，则
   * {@link SparkLauncher}类用于启动Spark应用程序。
   *   <li>"spark-class": if another class is provided, an internal Spark class is run.
   *   如果提供了另一个类，则运行内部Spark类</li>
   * </ul>
   *
   * This class works in tandem with the "bin/spark-class" script on Unix-like systems, and
   * "bin/spark-class2.cmd" batch script on Windows to execute the final command.
   * 该类与Unix系统上的“bin / spark-class”脚本和Windows上的“bin / spark-class2.cmd”批处理脚本一起工作，以执行最终命令。
   * <p>
   * On Unix-like systems, the output is a list of command arguments, separated by the NULL
   * character. On Windows, the output is a command line suitable for direct execution from the
   * script.
   * 在类Unix系统上，输出是由NULL分隔的命令参数列表字符,在Windows上，输出是适合从脚本直接执行的命令行。
   */
  public static void main(String[] argsArray) throws Exception {
    checkArgument(argsArray.length > 0, "Not enough arguments: missing class name.");
    //Arrays.asList将数组转List
    List<String> args = new ArrayList<String>(Arrays.asList(argsArray));

    String className = args.remove(0);

    boolean printLaunchCommand = !isEmpty(System.getenv("SPARK_PRINT_LAUNCH_COMMAND"));
    AbstractCommandBuilder builder;
    if (className.equals("org.apache.spark.deploy.SparkSubmit")) {
      try {
        builder = new SparkSubmitCommandBuilder(args);
      } catch (IllegalArgumentException e) {
        printLaunchCommand = false;
        System.err.println("Error: " + e.getMessage());
        System.err.println();

        MainClassOptionParser parser = new MainClassOptionParser();
        try {
          parser.parse(args);
        } catch (Exception ignored) {
          // Ignore parsing exceptions.
        }

        List<String> help = new ArrayList<String>();
        if (parser.className != null) {
          help.add(parser.CLASS);
          help.add(parser.className);
        }
        help.add(parser.USAGE_ERROR);
        builder = new SparkSubmitCommandBuilder(help);
      }
    } else {
      builder = new SparkClassCommandBuilder(className, args);
    }

    Map<String, String> env = new HashMap<String, String>();
    List<String> cmd = builder.buildCommand(env);
    if (printLaunchCommand) {
      System.err.println("Spark Command: " + join(" ", cmd));
      System.err.println("========================================");
    }

    if (isWindows()) {
      System.out.println(prepareWindowsCommand(cmd, env));
    } else {
      // In bash, use NULL as the arg separator since it cannot be used in an argument.
        //在bash中,使用NULL作为arg分隔符,因为它不能在参数中使用,
      List<String> bashCmd = prepareBashCommand(cmd, env);
      for (String c : bashCmd) {
        System.out.print(c);
        System.out.print('\0');
      }
    }
  }

  /**
   * Prepare a command line for execution from a Windows batch script.
   * 从Windows批处理脚本准备执行命令行
   *
   * The method quotes all arguments so that spaces are handled as expected. Quotes within arguments
   * are "double quoted" (which is batch for escaping a quote). This page has more details about
   * quoting and other batch script fun stuff: http://ss64.com/nt/syntax-esc.html
   * 该方法引用的所有参数，空间处理的预期。参数中的引号是“双引号”（这是用于转义引号的批处理）
   *  此页面有关于引用和其他批处理脚本的更多细节有趣的东西：http://ss64.com/nt/syntax-esc.html
   */
  private static String prepareWindowsCommand(List<String> cmd, Map<String, String> childEnv) {
    StringBuilder cmdline = new StringBuilder();
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
      cmdline.append(" && ");
    }
    for (String arg : cmd) {
      cmdline.append(quoteForBatchScript(arg));
      cmdline.append(" ");
    }
    return cmdline.toString();
  }

  /**
   * Prepare the command for execution from a bash script. The final command will have commands to
   * set up any needed environment variables needed by the child process.
   * 从bash脚本准备执行命令,最终命令将有命令来设置子进程所需的任何需要的环境变量,
   */
  private static List<String> prepareBashCommand(List<String> cmd, Map<String, String> childEnv) {
    if (childEnv.isEmpty()) {
      return cmd;
    }

    List<String> newCmd = new ArrayList<String>();
    newCmd.add("env");

    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }
    newCmd.addAll(cmd);
    return newCmd;
  }

  /**
   * A parser used when command line parsing fails for spark-submit. It's used as a best-effort
   * at trying to identify the class the user wanted to invoke, since that may require special
   * usage strings (handled by SparkSubmitArguments).
   * 命令行解析失败的解析器,用于spark-submit,它被用作尝试识别用户想要调用的类的尽力而为,
   * 因为这可能需要特殊的使用字符串（由SparkSubmitArguments处理）。
   */
  private static class MainClassOptionParser extends SparkSubmitOptionParser {

    String className;

    @Override
    protected boolean handle(String opt, String value) {
      if (opt == CLASS) {
        className = value;
      }
      return false;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }

  }

}
