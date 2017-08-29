package scalaDemo

import java.io.File
import java.net.URL

/**
  * Created by liush on 17-7-21.
  */
object GetResourcePathDemo extends  App{

  val file=getClass.getClassLoader.getResource("hdfs-site.xml").getFile
  println(file)
  //Thread.currentThread().getContextClassLoader,可以获取当前线程的引用,getContextClassLoader用来获取线程的上下文类加载器
  //getResource得到的是当前类class文件的URI目录,不包括自己
  val url = Thread.currentThread.getContextClassLoader.getResource("hdfs-site.xml")
  // final URL testLocalResource =ThreadLocal.class.getClassLoader().getResource("core-site.xml");
  System.out.println("==========" + url)
  val HADOOP_HOME = "/opt/cloudera/parcels/CDH/lib/hadoop/"
  val HADOOP_CONF_DIR = "/etc/hadoop/conf/"
  // //getResource得到的是当前类class文件的URI目录,不包括自己/根目录
  val keyStorePath = new File(this.getClass.getResource("/keystore").toURI).getAbsolutePath

  //String path=Thread.currentThread().getContextClassLoader().getResource("core-site.xml").toURI().getPath();
  val path = Thread.currentThread.getContextClassLoader.getResource("hdfs-site.xml").toURI.getPath
  //final String HADOOP_CONF_HDFS_SITE = ConfigurationManager.getHadoopConfDir() + "/hdfs-site.xml";
  //toURI此方法不会自动转义 URL 中的非法字符,
  //将抽象路径名转换为 URL：首先通过 toURI 方法将其转换为 URI，然后通过 URI.toURL 方法将 URI 装换为 URL
  System.out.println("===" + new File(path).getParent)
  System.out.println(path + "===" + new File(path).getAbsoluteFile.toURI.toURL + "===" + new File(path).getAbsoluteFile.toURI)
}
