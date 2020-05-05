import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("My app").master("local").getOrCreate()
    import spark.implicits._
    val textFile = "C:\\tmp\\data\\pg10.txt"
    val bookDF = spark.read.text(textFile)
//    df.printSchema()
//    df.select("value").show()
    val wordsDS = bookDF.flatMap(row => row(0).asInstanceOf[String].split(" "))
    val wordCount = wordsDS.groupBy("value").count()
//    wordCount.orderBy($"count".desc).show(10)
//    wordCount.orderBy($"count".asc).show(10)
    wordCount.orderBy($"count".desc).write.parquet("bible.parquet")

//    val sc = new SparkContext(conf)
//    val textData = sc.textFile("C:\\tmp\\data\\pg10.txt")
//    val wCount = textData.flatMap(line => line.split(" "))
//    var mapOP = wCount.map(w => (w, 1))
//    var reduceOP = mapOP.reduceByKey(_ + _).map(t => t.toString())
//    reduceOP.saveAsTextFile("C:\\tmp\\data\\pg10_result.txt")
  }
}

// реализовать word count через DF и RDD
// сохранить в паркет или в csv

// df.write.parquete