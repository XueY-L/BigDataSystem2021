import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class Matrix(private val data:Array[Double],private val rownum:Int){
    val colnum = (data.length.toDouble/rownum).ceil.toInt
    private val matrix:Array[Array[Double]]={ 
        val matrix:Array[Array[Double]] = Array.ofDim[Double](rownum,colnum)
        for(i <- 0 until rownum){
            for(j <- 0 until colnum){
                val index = i * colnum + j
                matrix(i)(j) = if(data.isDefinedAt(index)) data(index) else 0
            }
        }
        matrix
    }

    override def toString = {
        var str = ""
        matrix.map((p:Array[Double]) => {p.mkString(" ")}).mkString("\n")
    }

    def mat(row:Int,col:Int) = {
        matrix(row - 1)(col - 1)
    }

    def *(a:Matrix):Matrix = {
        if(this.colnum != a.rownum){
            val data:ArrayBuffer[Double] = ArrayBuffer()
            return new Matrix(data.toArray,this.rownum)
        }else{
            val data:ArrayBuffer[Double] = ArrayBuffer()
            for(i <- 0 until this.rownum){
                for(j <- 0 until a.colnum){
                    var num = 0.0
                    for(k <- 0 until this.colnum){
                        num += this.matrix(i)(k) * a.matrix(k)(j)
                    }
                data += num
                }
            }
        return new Matrix(data.toArray,this.rownum)
        }
    }

    def *(a:Double):Matrix = {
        val data:ArrayBuffer[Double] = ArrayBuffer()
        for(i <- 0 until this.rownum){
            for(j <- 0 until this.colnum){
                data += this.matrix(i)(j) * a
            }
        }
        return new Matrix(data.toArray,this.rownum)
    }

    def +(a:Matrix):Matrix = {
        if(this.rownum != a.rownum || this.colnum != a.colnum){
            val data:ArrayBuffer[Double] = ArrayBuffer()
            return new Matrix(data.toArray,this.rownum)
        }else{
            val data:ArrayBuffer[Double] = ArrayBuffer()
            for(i <- 0 until this.rownum){
                for(j <- 0 until this.colnum){
                    data += this.matrix(i)(j) + a.matrix(i)(j)
                }
            }
            return new Matrix(data.toArray,this.rownum)
        }       
    }

    def -(a:Matrix):Matrix = {
        if(this.rownum != a.rownum || this.colnum != a.colnum){
            val data:ArrayBuffer[Double] = ArrayBuffer()
            return new Matrix(data.toArray,this.rownum)
        }else{
            val data:ArrayBuffer[Double] = ArrayBuffer()
            for(i <- 0 until this.rownum){
                for(j <- 0 until this.colnum){
                    data += this.matrix(i)(j) - a.matrix(i)(j)
                }
            }
            return new Matrix(data.toArray,this.rownum)
        }       
    }

    def transpose():Matrix = {
        val transposeMatrix = for (i <- Array.range(0,colnum)) yield {
             for (rowArray <- this.matrix) yield rowArray(i)
            }
        return new Matrix(transposeMatrix.flatten,colnum)
    }

    def cov() = {
        val data:ArrayBuffer[Double] = ArrayBuffer()
        for(i <- 0 until this.transpose.rownum){
            for(j <- 0 until this.colnum){
                var num = 0.0
                for(k <- 0 until this.transpose.colnum){
                    num += this.transpose.matrix(i)(k) * this.matrix(k)(j)
                }
            data += num
            }
        }
        new Matrix(data.toArray,this.transpose.rownum)*(1.toDouble/this.rownum)
    }

    def mean() = {
        val meanMatrix:Array[Array[Double]] = Array.ofDim[Double](rownum,colnum)
        val propertyMean:Array[Double] = new Array[Double](colnum)
        for(j <- 0 until colnum){
            var propertyValueSum = 0.0
            for(i <- 0 until rownum){
                propertyValueSum += this.matrix(i)(j)
            }
            propertyMean(j) = propertyValueSum/rownum
        }
        for(j <- 0 until colnum){
            for(i <- 0 until rownum){
                meanMatrix(i)(j) = this.matrix(i)(j) - propertyMean(j)
            }
        }
        new Matrix(meanMatrix.flatten,rownum)
    }
}

var x = new Array[Double](3000)
var y = new Array[Double](1000)

val lines = Source.fromFile("/home/dsjxtjc/2021214316/EXP3/Task5/dataset.txt").getLines

var i = 0 
for (line <- lines){
    val tmpx =line.split(',')(0)
    y(i) = line.split(',')(1).toDouble
    for (j <- 0 to 2) 
        x(i*3+j) = tmpx(j).toDouble
    i = i + 1
}

var matX = new Matrix(x,1000)
var matY = new Matrix(y,1000)
val w = new Array[Double](3)
var matW = new Matrix(w,3)

val lr = 0.0000001
val epoch = 50

for (j <- 1 to epoch){
    val maty:Matrix = matX * matW - matY
    val g:Matrix = matX.transpose * maty
    val loss = maty.transpose * maty * (1.0 / (2 * 1000))
    matW = matW - g * lr
    println(loss)
}