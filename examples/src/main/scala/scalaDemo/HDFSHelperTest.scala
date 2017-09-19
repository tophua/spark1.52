package scalaDemo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer
/**
  * Created by liush on 17-7-17.
  * FileSystem定义了hadoop的一个文件系统接口
  */
object HDFSHelperTest {
  def start(args: Array[String]): Unit = {
    val hdfs : FileSystem = FileSystem.get(new Configuration)
    args(0) match {
      case "list" => traverse(hdfs, args(1))
      case "createFile" => HDFSHelper.createFile(hdfs, args(1))
      case "createFolder" => HDFSHelper.createFolder(hdfs, args(1))
      case "copyfile" => HDFSHelper.copyFile(hdfs, args(1), args(2))
      case "copyfolder" => HDFSHelper.copyFolder(hdfs, args(1), args(2))
      case "delete" => HDFSHelper.deleteFile(hdfs, args(1))
      case "copyfilefrom" => HDFSHelper.copyFileFromLocal(hdfs, args(1), args(2))
      case "copyfileto" => HDFSHelper.copyFileToLocal(hdfs, args(1), args(2))
      case "copyfolderfrom" => HDFSHelper.copyFolderFromLocal(hdfs, args(1), args(2))
      case "copyfolderto" => HDFSHelper.copyFolderToLocal(hdfs, args(1), args(2))
    }
  }

  def traverse(hdfs : FileSystem, hdfsPath : String) = {
    val holder : ListBuffer[String] = new ListBuffer[String]
    val paths : List[String] = HDFSHelper.listChildren(hdfs, hdfsPath, holder).toList
    for(path <- paths){
      //path.toString才是文件的全路径名
      System.out.println("--------- path = " + path)
      //path.getName只是文件名，不包括路径
      System.out.println("--------- Path.getname = " + new Path(path).getName)
    }
  }
}
