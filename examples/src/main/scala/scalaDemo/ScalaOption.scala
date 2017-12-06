import scala.collection.mutable.ArrayBuffer

/**
  * Created by liush on 17-12-5
  * scala常用操作
  */
object CollectionOption {

  def main(args: Array[String]) {

    println("=========================常用操作符===================")
    val left = List(1,2,3)
    val right = List(4,5,6)

    //以下操作等价
    //从列表的尾部添加另外一个列表
    println(left ++ right)   // List(1,2,3,4,5,6)
    println(left ++: right)  // List(1,2,3,4,5,6)
    //:: ::在列表的头部添加一个元素
    println(right.++:(left))    // List(1,2,3,4,5,6)
    println(right.:::(left))  // List(1,2,3,4,5,6)

    //以下操作等价
    //在列表的头部添加一个元素
    println(0 +: left)    //List(0,1,2,3)
    println(left.+:(0))   //List(0,1,2,3)
    //在列表的尾部添加一个元素
    //以下操作等价
    println(left :+ 4)    //List(1,2,3,4)
    println(left.:+(4))   //List(1,2,3,4)

    //以下操作等价
    //在列表的头部添加一个元素
    println(0 :: left)      //List(0,1,2,3)
    println(left.::(0))     //List(0,1,2,3)
    println("====================================================")

    println("=========================map操作======================")
    val nums = List(1,2,3)
    val square = (x: Int) => x*x
    println(nums.map(num => num*num))    //List(1,4,9)
    println(nums.map(math.pow(_,2)))     //List(1.0, 4.0, 9.0)
    println(nums.map(square))            //List(1,4,9)
    println("====================================================")


    println("======================flatmap操作====================")
    val text = List("A,B,C","D,E,F")
    //map变换应用到列表的每个元素中,原列表不变,返回一个新的列表数据
    val textMapped = text.map(_.split(",").toList) // List(List(A, B, C), List(D, E, F))
    println(textMapped)
    //flatten将所有嵌套的集合的元素一一取出逐一放置到一个集合中
    val textFlattened = textMapped.flatten          // List(A, B, C, D, E, F)
    println(textFlattened)
    //对于嵌套的集合(即集合中的元素还是集合),如果我们希望把每一个嵌套的子集“转换”成一个新的子集
    val textFlatMapped = text.flatMap(_.split(",").toList) // List(A, B, C, D, E, F)
    println(textFlatMapped)
    println("====================================================")


    println("=======================reduce操作=====================")
    //nums = List(1,2,3)
    //reduce这种二元操作对集合中的元素进行归约
    //表示从列表头部开始,对两两元素进行求和操作,下划线是占位符,用来表示当前获取的两个元素,两个下划线之间的是操作符,表示对两个元素进行的操作
    //最终把列表合并成单一元素
    println(nums.reduce((a,b) => a+b))   //6
    println(nums.reduce(_+_))            //6
    println(nums.sum)                 //6

    val doubles = List(2.0,2.0,3.0)
    //reduceLeft从列表的左边往右边应用reduce函数
    //reduceLeft和reduceRight都是针对两两元素进行操作,
    //reduceRight表示从列表尾部开始,对两两元素进行求和操作
    val resultLeftReduce = doubles.reduceLeft(math.pow)  // = pow( pow(2.0,2.0) , 3.0) = 64.0
    println(resultLeftReduce)
    //reduceRight表示从列表尾部开始,对两两元素进行求和操作
    val resultRightReduce = doubles.reduceRight(math.pow) // = pow(2.0, pow(2.0,3.0)) = 256.0
    println(resultRightReduce)
    println("====================================================")

    println("=========================fold操作======================")
    val nums2 = List(2,3,4)
    //带有初始值的reduce,从一个初始值开始,从左向右将两个元素合并成一个,最终把列表合并成单一元素
    println(nums2.fold(1)(_+_))  // = 1+2+3+4 = 10

    val nums3 = List(2.0,3.0)
    //带有初始值的reduceLeft
    println(nums3.foldLeft(4.0)(math.pow)) // = pow(pow(4.0,2.0),3.0) = 4096
    //带有初始值的reduceRight
    println(nums3.foldRight(1.0)(math.pow)) // = pow(1.0,pow(2.0,3.0)) = 8.0
    println("====================================================")

    println("=========================filter操作====================")
    val nums4 = List(1,2,3,4)
    //filter 保留列表中符合条件p的列表元素
    val odd = nums4.filter( _ % 2 != 0) // List(1,3)
    println(odd)
    //filterNot保留列表中不符合条件p的列表元素
    val even = nums4.filterNot( _ % 2 != 0) // List(2,4)
    println(even)
    println("====================================================")

    println("=============diff, union, intersect操作===============")
    val nums5 = List(1,2,3)
    val nums6 = List(2,3,4)
    //差集
    val diff1 = nums5 diff nums6   // List(1)

    println(diff1)
    val diff2 = nums6.diff(nums5)   // List(4)
    println(diff2)
    //并集,同操作符 ++
    val union1 = nums5 union nums6  // List(1,2,3,2,3,4)
    println(union1)
    val union2 = nums6 ++ nums5        // List(2,3,4,1,2,3)
    println(union2)
    //交集
    val intersection = nums5 intersect nums6  //List(2,3)
    println(intersection)
    println("====================================================")

    println("==================groupBy, grouped操作================")
    val data = List(("HomeWay","Male"),("XSDYM","Femail"),("Mr.Wang","Male"))
    //按条件分组,条件由 f 匹配,返回值是Map类型,每个key对应一个序列
    val group1 = data.groupBy(_._2)    //  Map("Male" -> List(("HomeWay","Male"),("Mr.Wang","Male")),"Female" -> List(("XSDYM","Femail")))
    println(group1)
    val group2 = data.groupBy{case (name,sex) => sex} //  Map("Male" -> List(("HomeWay","Male"),("Mr.Wang","Male")),"Female" -> List(("XSDYM","Femail")))
    println(group2)
    val fixSizeGroup = data.grouped(2).toList         //  List(List((HomeWay,Male), (XSDYM,Femail)), List((Mr.Wang,Male)))
    println(fixSizeGroup)
    println("====================================================")


    println("========================scan操作=====================")
    val nums7 = List(1,2,3)
    //scan会把每一步的计算结果放到一个新的集合中返回,而 fold 返回的是单一的值
    println(nums7.scan(10)(_+_))   // List(10,10+1,10+1+2,10+1+2+3) = List(10,11,13,16)

    val nums8 = List(1.0,2.0,3.0)
    //从左向右计算
    println(nums8.scanLeft(2.0)(math.pow))   // List(2.0,pow(2.0,1.0), pow(pow(2.0,1.0),2.0),pow(pow(pow(2.0,1.0),2.0),3.0) = List(2.0,2.0,4.0,64.0)
    //从右向左计算
    println(nums8.scanRight(2.0)(math.pow))  // List(2.0,pow(3.0,2.0), pow(2.0,pow(3.0,2.0)), pow(1.0,pow(2.0,pow(3.0,2.0))) = List(1.0,512.0,9.0,2.0)

    val asegment = Array(1,2,3,1,1,1,1,1,4,5)
    //从序列的 from 处开始向后查找，所有满足 p 的连续元素的长度
    val bsegment = asegment.segmentLength( {x:Int => x < 3},3)  // 5

    println("====================================================")


    println("=========================take操作======================")
    val nums9 = List(1,1,1,1,4,4,4,4)
    //返回当前序列中前 n 个元素组成的序列
    println(nums9.take(4))                     // List(1,1,1,1)
    //返回当前序列中，从右边开始，选择 n 个元素组成的序列
    println(nums9.takeRight(4))                // List(4,4,4,4)
    //返回当前序列中,从第一个元素开始,满足条件的连续元素组成的序列
    println(nums9.takeWhile( _ == nums.head))  // List(1,1,1,1)
    println("====================================================")


    println("========================drop操作=====================")

    val nums10 = List(1,1,1,1,4,4,4,4)
    //drop将当前序列中前 n 个元素去除后，作为一个新序列返回
    println(nums10.drop(4))                     // List(4,4,4,4)
    //掉尾部的 n 个元素
    println(nums10.dropRight(4))                // List(1,1,1,1)
    //去除当前数组中符合条件的元素,这个需要一个条件,就是从当前数组的第一个元素起,就要满足条件,
    // 直到碰到第一个不满足条件的元素结束（即使后面还有符合条件的元素）,否则返回整个数组
    println(nums10.dropWhile( _ == nums.head))  // List(4,4,4,4)
    println("====================================================")


    println("===============span,splitAt,partition操作=============")
    val nums11 = List(1,1,1,2,3,2,1)
    //分割序列为两个集合,从第一个元素开始,直到找到第一个不满足条件的元素止,之前的元素放到第一个集合,其它的放到第二个集合
    val (prefix,suffix) = nums11.span( _ == 1) // prefix = List(1,1,1), suffix = List(2,3,2,1)
    println((prefix,suffix))
    //从指定位置开始,把序列拆分成两个集合
    val (prefix2,suffix2) = nums11.splitAt(3)  // prefix = List(1,1,1), suffix = List(2,3,2,1)
    println((prefix2,suffix2))
    //按条件将序列拆分成两个新的序列,满足条件的放到第一个序列中,其余的放到第二个序列
    val (prefix3,suffix3) = nums11.partition( _ == 1) // prefix = List(1,1,1,1), suffix = List(2,3,2)
    println((prefix3,suffix3))

    println("====================================================")

    println("========================padTo操作=====================")
    val nums12 = List(1,1,1)
    //后补齐序列，如果当前序列长度小于 len，那么新产生的序列长度是 len，多出的几个位值填充 elem，如果当前序列大于等于 len ，则返回当前序列
   //需要一个长度为6的新序列,空出的填充 2
    println(nums12.padTo(6,2))
    println("====================================================")


    println("=============combinations&permutations操作============")
    val nums13 = List(1,1,3)
    //排列组合，他与combinations不同的是，组合中的内容可以相同，但是顺序不能相同，combinations不允许包含的内容相同，即使顺序不一样
    println(nums13.combinations(2).toList)     //List(List(1,1),List(1,3))
    val a = Array(1, 2, 3, 4, 5)
    val b = a.permutations.toList   // b 中将有120个结果，知道排列组合公式的，应该不难理解吧
    //println(b(","))
    println(nums13.permutations.toList)        // List(List(1,1,3),List(1,3,1),List(3,1,1))
    println("====================================================")

    println("=============product操作============")

    //返回所有元素乘积的值
    val bb = a.product       // b = 120  （1*2*3*4*5）
    println(bb)
    println("====================================================")

    println("========================zip操作======================")

    val alphabet = List("A","B","C")
    val nums14 = List(1,2)
    //将两个序列对应位置上的元素组成一个pair序列
    val zipped = alphabet zip nums14                 // List(("A",1),("B",2))
    println(zipped)
    val zippedAll = alphabet.zipAll(nums14,"*",-1)   // List(("A",1),("B",2),("C",-1))
    println(zippedAll)
    //序列中的每个元素和它的索引组成一个序列
    val zippedIndex = alphabet.zipWithIndex  // List(("A",0),("B",1),("C",2))
    println(zippedIndex)
    //unzip将含有两个元素的数组,第一个元素取出组成一个序列,第二个元素组成一个序列
    val (list1,list2) = zipped.unzip        // list1 = List("A","B"), list2 = List(1,2)
    println((list1,list2))
    //unzip3将含有三个元素的三个数组,第一个元素取出组成一个序列,第二个元素组成一个序列,第三个元素组成一个序列
    val (l1,l2,l3) = List((1, "one", '1'),(2, "two", '2'),(3, "three", '3')).unzip3   // l1=List(1,2,3),l2=List("one","two","three"),l3=List('1','2','3')
    println((l1,l2,l3))

    //同 zip ,但是允许两个序列长度不一样,不足的自动填充,如果当前序列端,空出的填充为 thisElem,如果 that 短,填充为 thatElem
    val zipa = Array(1,2,3,4,5,6,7)
    val zipb = Array(5,4,3,2,1)
    val zipAllc = zipa.zipAll(zipb,9,8)         //(1,5),(2,4),(3,3),(4,2),(5,1),(6,8),(7,8)


    val azipb = Array(1,2,3,4)
    val bzipb = Array(5,4,3,2,1)
    val cbzipb = azipb.zipAll(bzipb,9,8)         //(1,5),(2,4),(3,3),(4,2),(9,1)

    println("====================================================")


    println("=======================slice操作=====================")
    val nums15 = List(1,2,3,4,5)
    //取出当前序列中,from 到 until 之间的片段
    println(nums15.slice(2,4))  //List(3,4)
    println("====================================================")


    println("======================sliding操作====================")
    val nums16 = List(1,1,2,2,3,3,4,4)
    //从第一个元素开始,每个元素和它后面的 size - 1 个元素组成一个数组,最终组成一个新的集合返回,当剩余元素不够 size 数,则停止
    //该方法,可以设置步进 step,第一个元素组合完后,下一个从上一个元素位置+step后的位置处的元素开始
    ////第一个从1开始,第二个从3开始,因为步进是 2
    val d = Array(1,2,3,4,5)
    val csliding = d.sliding(3,2).toList   //第一个从1开始， 第二个从3开始，因为步进是 2
    for(i<-0 to csliding.length - 1){
      /**
      第0个：1,2,3
      第1个：3,4,5**/
      val s = "第%d个：%s"
      println(s.format(i,b(i).mkString(",")))
    }
    val groupStep1 = nums16.sliding(2,2).toList  //List(List(1,1),List(2,2),List(3,3),List(4,4))
    println(groupStep1)
    //从第一个元素开始,每个元素和它后面的size-1个元素组成一个数组,最终组成一个新的集合返回,当剩余元素不够size数,则停止
    val groupStep2 = nums16.sliding(2).toList //List(List(1,1),List(1,2),List(2,2),List(2,3),List(3,3),List(3,4),List(4,4))
    println(groupStep2)
    println("====================================================")


    println("======================updated操作====================")
    val nums17 = List(1,2,3,3)
    //将序列中 i 索引处的元素更新为 x ,并返回替换后的数组
    val fixed = nums17.updated(3,4)  // List(1,2,3,4)
    println(fixed)
    println("====================================================")

    println("======================view操作====================")
    val view = Array(1,2,3,4,5)
    val db = view.view(1,3)
    //返回 from 到 until 间的序列，不包括 until 处的元素
    println(view.mkString(",")) //2,3
    println("====================================================")


    println("======================transpose操作====================")
    val chars = Array(Array("a","b"),Array("c","d"),Array("e","f"))
    //矩阵转换,二维数组行列转换
    val transpose = chars.transpose
    println(transpose.mkString(","))
    println("====================================================")

    println("======================toMap操作====================")

    val charstoMap = Array(("a","b"),("c","d"),("e","f"))
    //Map类型,需要被转化序列中包含的元素时Tuple2类型数据
    val bcharstoMap = charstoMap.toMap
    println(bcharstoMap)      //Map(a -> b, c -> d, e -> f)
    println("====================================================")

    println("======================tail操作====================")
    val btaila = Array(1,2,3,4,5)
    //返回除了当前序列第一个元素的其它元素组成的序列
    val btail = btaila.tail      //  2,3,4,5
    println(btail)
    //序列求和
    val bsum = a.sum       //  15
    println("====================================================")

    println("======================subSequence操作====================")
    val subSequence = Array('a','b','c','d')
    //返回 start 和 end 间的字符序列
    val bsubSequence = subSequence.subSequence(1,3)
    println(bsubSequence.toString)     //  bc

    println("======================startsWith操作====================")
    val startsWitha = Array(0,1,2,3,4,5)
    val startsWithb = Array(1,2)
    //从指定偏移处，是否以某个序列开始
    println(startsWitha.startsWith(startsWithb,1))      //  true
    val astartsWith = Array(1,2,3,4,5)
    val bstartsWith = Array(1,2)
    //是否以某个序列开始
    println(astartsWith.startsWith(bstartsWith))        //  true
    println("====================================================")

    println("======================sorted操作====================")
    val asorted = Array(3,2,1,4,5)
    val bsorted = asorted.sorted
    println(bsorted.mkString(","))    // 1,2,3,4,5
    //自定义排序方法 lt
    val asortedd = asorted.sortWith(_.compareTo(_) > 0)  // 大数在前
    println(asortedd.mkString(","))    // 5,4,3,2,1
    //按指定的排序规则排序
    val basorted = asorted.sortBy( {x:Int => x})
    println(basorted.mkString(","))    // 1,2,3,4,5

    println("====================================================")


    println("======================sameElements操作====================")
    val samea = Array(1,2,3,4,5)
    val sameb = Array(1,2,3,4,5)
    //判断两个序列是否顺序和对应位置上的元素都一样
    println(samea.sameElements(sameb))  // true

    val samec = Array(1,2,3,5,4)
    //判断两个序列是否顺序和对应位置上的元素都一样
    println(samea.sameElements(samec))  // false
    println("====================================================")

    println("======================reverse操作====================")
    val reversea = Array(1,2,3,4,5)
    //反转序列
    val reverseb = reversea.reverse
    println(reverseb.mkString(","))    //5,4,3,2,1
    //反向生成迭代
    val reverseMapa = Array(1,2,3,4,5)
    //同 map 方向相反
    val reverseMapb = reverseMapa.reverseMap( {x:Int => x*10} )
    println(reverseMapb.mkString(","))    // 50,40,30,20,10
    println("====================================================")


    println("======================prefixLength操作====================")
      val prefixLengtha = Array(1,2,3,4,1,2,3,4)
    //给定一个条件p,返回一个前置数据列表的长度,这个数据列表中的元素都满足 p
    val prefixLengthb = prefixLengtha.prefixLength( {x:Int => x<3}) // b = 2
    println("====================================================")


    println("======================patch操作====================")
    //批量替换,从原序列的 from 处开始,后面的 replaced 数量个元素,将被替换成序列 that
    val apatch = Array(1, 2, 3, 4, 5)
    val bpatch = Array(3, 4, 6)
    val cpatch = apatch.patch(1,bpatch,2)
    println(cpatch.mkString(","))    // return 1,3,4,6,4,5
    /**从a的第二个元素开始,取两个元素,即2和3,这两个元素被替换为b的内容*/
    println("====================================================")

    val para = Array(1, 2, 3, 4, 5)
    //返回一个并行实现，产生的并行序列，不能被修改
    val parb = para.par   //  "ParArray" size = 5
    //nonEmpty 判断序列不是空
    println(para.nonEmpty)


    println("======================mkString操作=============相减=======")
    val mkStringa = Array(1, 2, 3, 4, 5)
    //将所有元素组合成一个字符串
    println(mkStringa.mkString) // return  12345
    //将所有元素组合成一个字符串，以 sep 作为元素间的分隔符
    println(mkStringa.mkString(","))    // return  1,2,3,4,5
    //将所有元素组合成一个字符串,以 start 开头,以 sep 作为元素间的分隔符,以 end 结尾
    println(mkStringa.mkString("{",",","}"))    // return  {1,2,3,4,5}
    println("====================================================")

    println("======================minBy,max,length,lastOption操作====================")
    //返回当前序列中最后一个对象
    println(mkStringa.lastOption)   // return  Some(5)
    //返回当前序列中元素个数
    println(mkStringa.length)   // return  5
    //返回序列中最大的元素
    println(mkStringa.max)  // return  5
    //minBy
    //返回序列中第一个符合条件的最大的元素
    println(mkStringa.maxBy( {x:Int => x > 2})) // return  3
    println("====================================================")


    println("======================lastIndexWhere,lastIndexOf操作====================")
    val alastIndexWhere = Array(1, 4, 2, 3, 4, 5, 1, 4)
    val blastIndexWhere = Array(1, 4)
    //返回当前序列中最后一个满足条件p的元素的索引,可以指定在 end 之前(包括)的元素中查找
    println(alastIndexWhere.lastIndexWhere( {x:Int => x<2},2))    // return  0
    //返回当前序列中最后一个满足条件 p 的元素的索引
    println(alastIndexWhere.lastIndexWhere( {x:Int => x<2}))  // return  6
    //判断当前序列中是否包含序列 that,并返回最后一次出现该序列的位置处的索引
    println(alastIndexWhere.lastIndexOfSlice(b))  // return  6
    //判断当前序列中是否包含序列 that，并返回最后一次出现该序列的位置处的索引，可以指定在 end 之前（包括）的元素中查找
    println(alastIndexWhere.lastIndexOfSlice(blastIndexWhere,4))    // return  0

    //取得序列中最后一个等于elem的元素的位置,可以指定在end之前(包括)的元素中查找
    println(alastIndexWhere.lastIndexOf(4,3)) // return  1

    //取得序列中最后一个等于 elem 的元素的位置
    val alastIndexOf = Array(1, 4, 2, 3, 4, 5)
    println(alastIndexOf.lastIndexOf(4))   // return  4
    println("====================================================")


    println("======================last,iterator,isTraversableAgain,isEmpty,isDefinedAt操作====================")
    val aArray = Array(1, 2, 3, 4, 5)
    //取得序列中最后一个元素
    println(aArray.last) // return  5

    val baArray = aArray.iterator  //此时就可以通过迭代器访问 b
    //判断序列是否可以反复遍历
    println(aArray.isTraversableAgain)
    //判断当前序列是否为空
    println(aArray.isEmpty)
    //nonEmpty 判断序列不是空
    println(aArray.nonEmpty)
    //判断序列中是否存在指定索引
    println(aArray.isDefinedAt(1))   // true
    println(aArray.isDefinedAt(10))  // false

    //返回当前序列中不包含最后一个元素的序列
    val inita = Array(10, 2, 3, 40, 5)
    val initb = inita.init
    println(initb.mkString(","))    // 10, 2, 3, 40val a = Array(1, 3, 2, 3, 4)
println(a.indexOf(3,2)) // return 3

    val indexWherea = Array(1, 2, 3, 4, 5, 6)
    //返回当前序列中第一个满足 p 条件的元素的索引
    println(indexWherea.indexWhere( {x:Int => x>3}))  // return 3
    //返回当前序列中第一个满足p条件的元素的索引,可以指定从from 索引处开始
    val aindexWherea = Array(1, 2, 3, 4, 5, 6)
    println(aindexWherea.indexWhere( {x:Int => x>3},4))    // return 4
    //
    val indexOfSlicea = Array(1, 3, 2, 3, 4)
    val indexOfSliceb = Array(2,3)
    //检测当前序列中是否包含另一个序列(that),并返回第一个匹配出现的元素的索引
    println(indexOfSlicea.indexOfSlice(indexOfSliceb))  // return 2
    println(indexOfSlicea.indexOfSlice(indexOfSliceb,3))    // return 4
    println("====================================================")


    println("======================indexOf,headOption,head,isEmpty,isDefinedAt,endsWith,diff操作====================")
    val Arraya = Array(1, 3, 2, 3, 4)
    //返回elem在序列中的索引，找到第一个就返回
    println(Arraya.indexOf(3))   // return 1
    //返回elem在序列中的索引,可以指定从某个索引处(from)开始查找,找到第一个就返回
    println(Arraya.indexOf(3,2)) // return 3
    //返回Option类型对象，就是scala.Some 或者 None,如果序列是空,返回None
    println(Arraya.headOption)   //Some(1)
    //返回序列的第一个元素，如果序列为空，将引发错误
    println(Arraya.head) //1
    //检测序列是否存在有限的长度，对应Stream这样的流数据，返回false
    println(Arraya.hasDefiniteSize)  //true

    val agrouped = Array(1, 2, 3,4,5)
    /**
    第1组:1,2,3
    第2组:4,5
    */
    //按指定数量分组，每组有 size 数量个元素，返回一个集合
    val bagrouped = agrouped.grouped(3).toList

    //查找第一个符合条件的元素
    //println(b)  //Some(3)
    val findb = Arraya.find( {x:Int => x>2} )
    println(findb)
    //取得当前数组中符合条件的元素，组成新的数组返回
    val bfilter = Arraya.filter( {x:Int => x> 2} )
    println(bfilter.mkString(","))    //3,3,4
    //判断当前数组是否包含符合条件的元素
    println(Arraya.exists( {x:Int => x==3} ))   //true
    println(Arraya.exists( {x:Int => x==30} ))  //false
    //判断是否以某个序列结尾

    val aArraya = Array(3, 2, 3,4)
    val bArraya = Array(3,4)
    //判断是否以某个序列结尾
    println(aArraya.endsWith(bArraya))  //true

    //将当前序列中前 n 个元素去除后，作为一个新序列返回
    val cbArraya = aArraya.drop(2)
    println(cbArraya.mkString(","))    // 3,4
    //统计符合条件的元素个数,下面统计大于 2 的元素个数
    println(agrouped.count({x:Int => x > 2}))  // count = 3
    //计算当前数组与另一个数组的不同。将当前数组中没有在另一个数组中出现的元素返回
    val caArray = aArraya.diff(bArraya)

    //去除当前集合中重复的元素，只保留一个
    val caArraya = aArraya.distinct
    println(caArraya.mkString(","))//2,3,4

    println("====================================================")



    println("======================copyToBuffer,copyToArray,acontainsSlice,combinations,isDefinedAt,endsWith操作====================")
    val arraya = Array('a', 'b', 'c')
    val Arrayab:ArrayBuffer[Char]  = ArrayBuffer()
    //将数组中的内容拷贝到Buffer中
    arraya.copyToBuffer(Arrayab)
    println(Arrayab.mkString(","))
    //copyToArray
    val bArray : Array[Char] = new Array(5)
    //将数组中的内容拷贝到数组
    arraya.copyToArray(bArray)    /**b中元素 ['a','b','c',0,0]*/
    arraya.copyToArray(bArray,1)  /**b中元素 [0,'a',0,0,0]*/
    arraya.copyToArray(bArray,1,2)    /**b中元素 [0,'a','b',0,0]*/
    val acontainsSlice = List(1,2,3,4)
    val bcontainsSlice = List(2,3)
    //判断当前序列中是否包含另一个序列
    println(acontainsSlice.containsSlice(bcontainsSlice))  //true
    //序列中是否包含指定对象
    println(arraya.contains('a'))
    //排列组合,这个排列组合会选出所有包含字符不一样的组合,对于 “abc”、“cba”，只选择一个,参数n表示序列长度，
    val arr = Array("a","b","c")
    val newarr = arr.combinations(2)
    newarr.foreach((item) => println(item.mkString(",")))
    /**
    a,b
    a,c
    b,c*/

    //在序列中查找第一个符合偏函数定义的元素，并执行偏函数计算
    val arrPartial = Array(1,'a',"b")
    //定义一个偏函数,要求当被执行对象为Int类型时，进行乘100的操作，对于上面定义的对象arr来说，只有第一个元素符合要求
    val fun:PartialFunction[Any,Int] = {
      case x:Int => x*100
    }
    //计算,在序列中查找第一个符合偏函数定义的元素,并执行偏函数计算
    val valuea = arrPartial.collectFirst(fun)
    println("value:"+valuea)
    //另一种写法
    val value = arrPartial.collectFirst({case x:Int => x*100})
    //判断两个对象是否可以进行比较
    println(arr.canEqual(arr))
    println(arr.canEqual(a))

    println("====================================================")


    val numbers = Array(1, 2, 3, 4) //声明一个数组对象
    val first = numbers(0) // 读取第一个元素
    numbers(3) = 100 // 替换第四个元素为100
    val biggerNumbers = numbers.map(_ * 2) // 所有元素乘2

    val aa = Array(1,2)
    val bbb = Array(3,4)
    //合并集合,并返回一个新的数组,新数组包含左右两个集合对象的内容
    val cc = aa ++ bbb
    //c中的内容是(1,2,3,4)

    val bbbb = scala.collection.mutable.LinkedList(3,4)
    //这个方法++一个方法类似,两个加号后面多了一个冒号,但是不同的是右边操纵数的类型决定着返回结果的类型。
    //下面代码中List和LinkedList结合,返回结果是LinkedList类型
    val ccc = aa ++: bbbb
    println(ccc.getClass().getName())// c的类型是：scala.collection.mutable.LinkedList

    //对数组中所有的元素进行相同的操作,foldLeft的简写
    val cd = (10 /: numbers)(_+_)   // 1+2+3+4+10
    val dc = (10 /: numbers)(_*_)   // 1*2*3*4*10
    println("c:"+cd)   // c:20
    println("d:"+dc)   // d:240

    val bString = new StringBuilder()
    //将数组中的元素逐个添加到b中
    val cnumbers = numbers.addString(bString)   // c中的内容是  1234
    //将数组中的元素逐个添加到b中,每个元素用sep分隔符分开
    val cnumberss = numbers.addString(bString,",")

    println("cc:  "+cnumberss)  // c:  1,2,3,4

    //在首尾各加一个字符串,并指定sep分隔符
    val caddString = numbers.addString(bString,"{",",","}")
    println("cz:  "+caddString)  // c:  {1,2,3,4}
    println("=====================ArrayBuffer操作===============================")


    //一个空的数组缓冲，准备存放整数
    val ab = ArrayBuffer[Int]()
    val ab2 = new ArrayBuffer[Int]

    //用+=在尾部添加元素
    ab += 2

    //在尾部添加多个元素
    ab += (1,2,3,4,5)

    //通过++=往数组缓冲后面追加集合
    ab ++= Array(6,7,8,9)
    ab2 += (1,2,3,4,5)
    //二个ArrayBuffer相减
    println("ab:"+ab.mkString+"==ab2=="+ab2.mkString+"==相减(--)=="+(ab--ab2).mkString(","))
    //使用trimEnd(n)移除尾部n个元素
    ab.trimEnd(3)

    //在下标3之前插入元素
    ab.insert(3, 33)

    //插入多个元素，第一个值为index，后面所有的值为要插入的值
    ab.insert(3,3,4,5,6)


    //移除某个位置的元素
    ab.remove(3)

    //移除从下标为n开始（包括n）的count个元素
    val n=1
    val count=4
    ab.remove(n, count)


    println("=====================Array操作===============================")
    //长度不变的数组的声明
    //长度为10的整数数组，所有元素初始化为0
    val numArra = new Array[Int](10)

    //长度为10的字符串数组，所有元素初始化为null
    val numArr = new Array[String](10)

    //长度为2的数组，数据类型自动推断出来，已经提供初始值就不需要new关键字
    val s = Array("cai","yong")

    //通过ArrayName(index)访问数组元素和更改数组元素
    val s1 = Array("cai","yong")
    println(s1(0))
    s1(0) = "haha"
    println(s1(0))

    println("=====================遍历数组操作===============================")
    //for循环遍历
    for(i <- 0 until ab.length){
      print(ab(i) + ", ")
    }

    //根据特定步长遍历数组
    for(i <- 0 until (ab.length, 2)){
      print(ab(i) + ", ")
    }

    //从数组的尾部开始向前遍历数组
    for(i <- (0 until ab.length).reverse){
      print(ab(i) + ", ")
    }

    //类似于Java中的foreach遍历数组
    for(elem <- ab){
      print(elem + ", ")
    }

    println("=====================数组转换操作===============================")
    //进行数组转换会生成一个新的数组，而不会修改原始数组
    val change = for(elem <- ab) yield elem * 2
    for(elem <- change){
      print(elem + ", ")
    }

    //添加一个守卫的数组转换
    val changea = for(elem <- ab if elem%2 == 0) yield elem * 2


    println("=====================数组操作常用算法操作===============================")

    //sum求和(数组与阿奴必须是数值型数据)
    println(change.sum)

    //min max 输出数组中最小和最大元素
    println(change.min)
    println(change.max)

    //使用sorted方法对数组或数组缓冲进行升序排序，这个过程不会修改原始数组
    val sortArra = ab.sorted
    for(elem <- sortArra)
      print(elem + ", ")

    //使用比较函数sortWith进行排序
    val sortArr = ab.sortWith(_>_)

    //数组显示
    println(sortArr.mkString("|"))
    println(sortArr.mkString("startFlag","|","endFlag"))

    println("=====================多维数组操作===============================")
    //构造一个2行3列的数组
    val arra = Array.ofDim[Int](2,3)
    println(arra.length)
    println(arra(0).length)
    arra(0)(0) = 20
    println(arra(0)(0))

    //创建长度不规则的数组
    val arrb = new Array[Array[Int]](3)

    for(i <- 0 until arrb.length){
      arrb(i) = new Array[Int](i + 2)
    }

    for(i <- 0 until arrb.length){
      println(arr(i).length)
    }

  }

}
