import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.Row

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class CellGrid(rdd: RDD[Array[Double]], divisionType: Int) extends Serializable {

  var intervals: Array[Double] = Array()
  var bounds: List[List[Double]] = List()
  var dims: Int = 0

  def double(n: Any): Double = {
    n.toString.toDouble
  }

  def makeGrid(): Unit = {
    this.dims = rdd.take(1)(0).length
    val flatMap = rdd.flatMap(x=>x).zipWithIndex().map(p=>(p._2%this.dims, p._1))
    val cols = flatMap.groupBy(p=>p._1).map(p=>p._2.toArray).map(x=>x.map(x=>x._2))
    val min_and_max = cols.map(arr=>(arr.min, arr.max))
    val bounds_0 = min_and_max.map(x => x._1).zipWithIndex().map(p=>(p._2, p._1))
    val intervals = min_and_max.map(x => (x._2-x._1) / divisionType).zipWithIndex().map(p=>(p._2, p._1))

    val bounds: List[List[Double]] = List.range(1, this.dims).map(c => (for (i <- 1 to divisionType)
      yield bounds_0.lookup(c).head + i * intervals.lookup(c).head).toList)

    this.bounds = bounds
    val interval_array = (for (i<-List.range(0, this.dims)) yield intervals.lookup(i).head).toArray
    this.intervals = interval_array
  }

  def findGridLines(): List[List[Double]] = this.bounds

  def findPartition(number: Row): Int = {
    val n = number.toSeq
    val base = divisionType
    val modified = n.map(x => if (x == 1.0) 0.995 else x)

    val reverseNumber = (for (i <- List.range(0, n.length)) yield (double(modified(i)) / intervals(i)).toInt).reverse
    val partition = (for (i <- List.range(0, n.length)) yield reverseNumber(i) * scala.math.pow(base, i)).sum.toInt
    partition.toInt
  }

  def numPartitions: Int = scala.math.pow(divisionType, dims).toInt

}

class CustomPartitioner(numOfPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  override def numPartitions: Int = numOfPartitions
}

object SFSSkylineCalculation extends Serializable {

  def calculate(a: Iterator[Array[Double]]): Iterator[Array[Double]] = {
    var skyline = ArrayBuffer[Array[Double]]()
    val array = a.toArray
    skyline += array(0)
    for(i <- 1 until array.length) {
      var toBeAdded = true
      var j = 0
      breakable{
        while(j < skyline.length){
          if(dominationCondition.dominates(array(i), skyline(j))){
            skyline.remove(j)
            j -= 1
          }
          else if (dominationCondition.dominates(skyline(j), array(i))){
            toBeAdded = false
            break()
          }
          j += 1
        }}
      if (toBeAdded) {
        skyline += array(i)
      }
    }
    skyline.toIterator
  }
  
  def addScore(array:Iterator[Array[Double]]): Iterator[(Array[Double], Double)] ={
    array.map(x => (x, 0))
      .map(x => {
        var sum = 0.0
        for (i<-0 to x._1.length - 1) {
          sum += math.log(x._1(i) + 1)
        }
        (x._1, sum)
      })
  }

  def sortByScore(iterator:Iterator[(Array[Double], Double)]): Iterator[(Array[Double], Double)] ={
    var array = iterator.toArray
    array.sortBy(x => - x._2)
    return array.toIterator
  }

  def addScoreAndCalculate(x: Iterator[(Array[Double])]): Iterator[Array[Double]] ={
    val score = addScore(x)
    val sortedScore = sortByScore(score)
    val result = calculate(sortedScore.map(x => x._1))
    return result.take(5)
  }
}

object dominationCondition extends Serializable {
  def dominates(a: Array[Double], b:Array[Double]): Boolean = {
    a.zip(b).forall(pair=>pair._1<=pair._2) && !a.deep.equals(b.deep)
  }
  def toBase(n: Int, b: Int): List[Double] ={
    @tailrec
    def loop(acc: List[Double], n: Int): List[Double]={
      if (n==0) acc
      else loop(acc:+(n%b).toDouble, n/b)
    }
    loop(List(), n).reverse
  }
  def dominates(a: Int, b:Int, base: Int, dims: Int): Boolean ={
    val upper = toBase(scala.math.pow(base, dims).toInt, base).length
    val a1 = toBase(a, base).toArray.reverse.padTo(upper, 0).reverse
    val b1 = toBase(b, base).toArray.reverse.padTo(upper, 0).reverse
    a1.zip(b1).forall(pair=>pair._1.toString.toDouble<pair._2.toString.toDouble) && !a1.deep.equals(b1.deep)
  }
}

object NonDominatedPartitions extends Serializable {
  def calculate(partitions: RDD[Int], divisionType: Int, dimensions: Int): Array[Int] = {
    val partitionsWithIndex = partitions.zipWithIndex().map(p=>(p._2, p._1))
    var nonDominated: Array[Int] = Array()
    val n = partitions.count()
    for (i<-List.range(0, n)) {
      var flag = true
      for (j<-List.range(0, i)) {
        if (dominationCondition.dominates(partitionsWithIndex.lookup(i).head,
          partitionsWithIndex.lookup(j).head,
          divisionType, dimensions)) {
          flag = false
        }
      }
      if (flag) nonDominated = nonDominated :+ i.toInt
    }
    nonDominated
  }
}

object Skyline {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Skyline Queries")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("gaussian.csv").map(x=>x.split(", ")).map(x => x.map( y => y.toDouble))

    val divisionType = 3

    val grid = new CellGrid(rdd, divisionType)
    grid.makeGrid()

    val dimensions = rdd.take(1)(0).length

    val partitions = rdd.map(p => grid.findPartition(Row.fromSeq(p))).distinct()

    //val pointsWithPartition = rdd.zip(partitions)
    //val nonEmptyPartitions = pointsWithPartition.map(p=>p._2).distinct()
    val nonEmptyPartitions = sc.parallelize(List(0, 1, 2, 3))

    val nonDominatedPartitions = NonDominatedPartitions.calculate(nonEmptyPartitions, divisionType, dimensions)
    nonDominatedPartitions.foreach(println)
    //val filteredPoints = rdd.filter(p=>nonDominatedPartitions.contains(grid.findPartition(Row.fromSeq(p.toSeq))))
    //println(filteredPoints.count())

    val partitionedFilteredPoints = rdd.map(x=>(grid.findPartition(Row.fromSeq(x)), x)).
      partitionBy(new CustomPartitioner(partitions.count().toInt)).map(p=>p._2)

    //List.range(0, grid.numPartitions).foreach(i=>println(df2.glom().collect()(i).length))

    // Test for ALS algorithm

    val rdd2 = rdd.mapPartitions(SFSSkylineCalculation.calculate)
    val partialSkylines2 = rdd2.collect()
    val skyline2 = sc.parallelize(partialSkylines2).repartition(1).mapPartitions(SFSSkylineCalculation.calculate)
    println("Default partitioning: number of local skylines: "+skyline2.count())

    //skyline2.map(row => (row.toArray.mkString(" "))).saveAsTextFile("ALS")

    val rdd3 = partitionedFilteredPoints.mapPartitions(SFSSkylineCalculation.calculate).collect()
    val partialSkylines3 = sc.parallelize(rdd3, 1).repartition(1)
    val skyline3 = partialSkylines3.mapPartitions(SFSSkylineCalculation.calculate)
    println("Grid partitioning: number of local skylines: "+skyline3.count())

    //skyline2.foreach(x=>println(x.toList))
    //skyline3.foreach(x=>println(x.toList))

    sc.stop()
  }
}
