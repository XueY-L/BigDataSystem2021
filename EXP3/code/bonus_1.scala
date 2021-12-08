import java.util.Date

val words = sc.textFile("/dsjxtjc/2021214316/newstest2016en.txt")
var t1 = new Date().getTime
val result = words.flatMap(l => l.split(" ")).countByValue()
var t2 = new Date().getTime
println(t2-t1)