import java.util.Date


val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount4")

val sc: SparkContext = new SparkContext(config)

val lines: RDD[String] = sc.textFile("/dsjxtjc/2021214316/newstest2016en.txt")

var t1 = new Date().getTime
val groupByKeyRDD: RDD[(String, Iterable[Int])] = lines.flatMap(_.split(" ")).map((_, 1)).groupByKey()

groupByKeyRDD.map(tuple => {
    (tuple._1, tuple._2.sum)
}).collect().foreach(println)
var t2 = new Date().getTime
println(t2-t1)