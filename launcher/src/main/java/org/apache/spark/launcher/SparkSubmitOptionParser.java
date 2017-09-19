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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for spark-submit command line options.
 * spark-submit命令行选项的解析器
 * <p>
 * This class encapsulates the parsing code for spark-submit command line options, so that there
 * is a single list of options that needs to be maintained (well, sort of, but it makes it harder
 * to break things).
 * 这个类封装了spark-submit命令行选项的解析代码,这样就可以了是需要维护的单个选项列表(好的,但这使得它更难打破事情)。
 */
class SparkSubmitOptionParser {

  // The following constants define the "main" name for the available options. They're defined
  // to avoid copy & paste of the raw strings where they're needed.
  //以下常量定义可用选项的“main”名称,它们被定义为避免在需要的地方复制和粘贴原始字符串,
  //
  // The fields are not static so that they're exposed to Scala code that uses this class. See
  // SparkSubmitArguments.scala. That is also why this class is not abstract - to allow code to
  // easily use these constants without having to create dummy implementations of this class.
    //这些字段不是静态的，因此它们暴露于使用此类的Scala代码。 看到SparkSubmitArguments.scala。
  // 这也是为什么这个类不是抽象的 - 允许代码容易地使用这些常量，而不必创建这个类的虚拟实现。
  protected final String CLASS = "--class";
  protected final String CONF = "--conf";
  protected final String DEPLOY_MODE = "--deploy-mode";
  protected final String DRIVER_CLASS_PATH = "--driver-class-path";
  protected final String DRIVER_CORES = "--driver-cores";
  protected final String DRIVER_JAVA_OPTIONS =  "--driver-java-options";
  protected final String DRIVER_LIBRARY_PATH = "--driver-library-path";
  protected final String DRIVER_MEMORY = "--driver-memory";
  protected final String EXECUTOR_MEMORY = "--executor-memory";
  protected final String FILES = "--files";
  protected final String JARS = "--jars";
  protected final String KILL_SUBMISSION = "--kill";
  protected final String MASTER = "--master";
  protected final String NAME = "--name";
  protected final String PACKAGES = "--packages";
  protected final String PACKAGES_EXCLUDE = "--exclude-packages";
  protected final String PROPERTIES_FILE = "--properties-file";
  protected final String PROXY_USER = "--proxy-user";
  protected final String PY_FILES = "--py-files";
  protected final String REPOSITORIES = "--repositories";
  protected final String STATUS = "--status";
  protected final String TOTAL_EXECUTOR_CORES = "--total-executor-cores";

  // Options that do not take arguments.
    //不采取参数的选项
  protected final String HELP = "--help";
  protected final String SUPERVISE = "--supervise";
  protected final String USAGE_ERROR = "--usage-error";
  protected final String VERBOSE = "--verbose";
  protected final String VERSION = "--version";

  // Standalone-only options.独立选项

  // YARN-only options. 仅限YARN选项
  protected final String ARCHIVES = "--archives";
  protected final String EXECUTOR_CORES = "--executor-cores";
  protected final String KEYTAB = "--keytab";
  protected final String NUM_EXECUTORS = "--num-executors";
  protected final String PRINCIPAL = "--principal";
  protected final String QUEUE = "--queue";

  /**
   * This is the canonical list of spark-submit options. Each entry in the array contains the
   * different aliases for the same option; the first element of each entry is the "official"
   * name of the option, passed to {@link #handle(String, String)}.
   *
   * 这是spark-submit选项的规范列表,数组中的每个条目都包含相同选项的不同别名;
   * 每个条目的第一个元素是选项的“官方”名称,传递给{@link #handle（String，String）}
   * <p>
   * Options not listed here nor in the "switch" list below will result in a call to
   * 此处未列出的选项和下面的“开关”列表将导致调用
   * @link $#handleUnknown(String).
   * <p>
   * These two arrays are visible for tests.
   * 这两个数组对于测试是可见的
   */
  final String[][] opts = {
    { ARCHIVES },
    { CLASS },
    { CONF, "-c" },
    { DEPLOY_MODE },
    { DRIVER_CLASS_PATH },
    { DRIVER_CORES },
    { DRIVER_JAVA_OPTIONS },
    { DRIVER_LIBRARY_PATH },
    { DRIVER_MEMORY },
    { EXECUTOR_CORES },
    { EXECUTOR_MEMORY },
    { FILES },
    { JARS },
    { KEYTAB },
    { KILL_SUBMISSION },
    { MASTER },
    { NAME },
    { NUM_EXECUTORS },
    { PACKAGES },
    { PACKAGES_EXCLUDE },
    { PRINCIPAL },
    { PROPERTIES_FILE },
    { PROXY_USER },
    { PY_FILES },
    { QUEUE },
    { REPOSITORIES },
    { STATUS },
    { TOTAL_EXECUTOR_CORES },
  };

  /**
   * List of switches (command line options that do not take parameters) recognized by spark-submit.
   * spark-submit识别的交换机列表（不采用参数的命令行选项）
   */
  final String[][] switches = {
    { HELP, "-h" },
    { SUPERVISE },
    { USAGE_ERROR },
    { VERBOSE, "-v" },
    { VERSION },
  };

  /**
   * Parse a list of spark-submit command line options.
   * 解析spark-submit命令行选项的列表
   * <p>
   * See SparkSubmitArguments.scala for a more formal description of available options.
   * 有关可用选项的更正式说明,请参阅SparkSubmitArguments.scala
   * @throws IllegalArgumentException If an error is found during parsing. 如果在解析期间发现错误
   */
  protected final void parse(List<String> args) {
      //正测表达式--[^=] 以--开头不包含=
    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    int idx = 0;
    for (idx = 0; idx < args.size(); idx++) {
      String arg = args.get(idx);
      String value = null;

      Matcher m = eqSeparatedOpt.matcher(arg);
      if (m.matches()) {
        arg = m.group(1);
        value = m.group(2);
      }

      // Look for options with a value. 查找具有值的选项
      String name = findCliOption(arg, opts);
      if (name != null) {
        if (value == null) {
          if (idx == args.size() - 1) {
            throw new IllegalArgumentException(
                String.format("Missing argument for option '%s'.", arg));
          }
          idx++;
          value = args.get(idx);
        }
        if (!handle(name, value)) {
          break;
        }
        continue;
      }

      // Look for a switch.
      name = findCliOption(arg, switches);
      if (name != null) {
        if (!handle(name, null)) {
          break;
        }
        continue;
      }

      if (!handleUnknown(arg)) {
        break;
      }
    }

    if (idx < args.size()) {
      idx++;
    }
    handleExtraArgs(args.subList(idx, args.size()));
  }

  /**
   * Callback for when an option with an argument is parsed.
   * 调用带有参数的选项时的回调
   *
   * @param opt The long name of the cli option (might differ from actual command line).
   * @param value The value. This will be <i>null</i> if the option does not take a value.
   * @return Whether to continue parsing the argument list.
   */
  protected boolean handle(String opt, String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Callback for when an unrecognized option is parsed.
   * 解析无法识别的选项时的回调
   *
   * @param opt Unrecognized option from the command line.
   * @return Whether to continue parsing the argument list.
   */
  protected boolean handleUnknown(String opt) {
    throw new UnsupportedOperationException();
  }

  /**
   * Callback for remaining command line arguments after either {@link #handle(String, String)} or
   * {@link #handleUnknown(String)} return "false". This will be called at the end of parsing even
   * when there are no remaining arguments.
   * 在{@link #handle（String，String）}之后的其他命令行参数回调{@link #handleUnknown（String）} return“false”。
   * 这将在解析结束时被调用当没有剩余的参数时
   *
   * @param extra List of remaining arguments.
   */
  protected void handleExtraArgs(List<String> extra) {
    throw new UnsupportedOperationException();
  }

  private String findCliOption(String name, String[][] available) {
    for (String[] candidates : available) {
      for (String candidate : candidates) {
        if (candidate.equals(name)) {
          return candidates[0];
        }
      }
    }
    return null;
  }

}
