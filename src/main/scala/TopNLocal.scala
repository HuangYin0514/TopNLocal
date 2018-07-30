import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopNLocal {

  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
  System.setProperty("HADOOP_USER_NAME", "hadoop")

  val conf = new SparkConf().setAppName("TopN").setMaster("local[2]")
  val sc = new SparkContext(conf)

  LoggerLevels.setStreamingLogLevels()

  def main(args: Array[String]): Unit = {
    val fileContext: RDD[String] = sc.textFile("C:\\Users\\10713\\IdeaProjects\\TopNLocal\\src\\main\\resources\\access.log")
    val contextSplit: RDD[Array[String]] = fileContext.map(_.split(" "))
    //    val refUrl: RDD[Array[String]] =
    val refUrl = contextSplit.map {
      case line =>
        line(10)
    }
    val urlAndOne = refUrl.map {
      case url => (url, 1)
    }
    val urlAndCount: RDD[(String, Int)] = urlAndOne.reduceByKey(_+_)
    val res: RDD[(String, Int)] = urlAndCount.sortBy(_._2,false)
    val tuplesAndTake5: Array[(String, Int)] = res.take(5)

    println(tuplesAndTake5.toList)


  }
}
