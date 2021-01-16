import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.Row

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class CellGrid(rdd: RDD[List[Double]], divisionType: Int) extends Serializable {

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

object SFSkylineCalculation extends Serializable {

  def calculate(a: Iterator[List[Double]]): Iterator[List[Double]] = {
    var skyline = ArrayBuffer[List[Double]]()
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

  def addScore(array:Iterator[List[Double]], scores: Map[List[Double], Int]): Iterator[(List[Double], Int)] ={
    val arr = array.toArray
    val result_scores = scala.collection.mutable.Map[List[Double], Int]()
    var i = 0
    for (x<-arr){
      var score = 0//scores(x)
      for (y<-arr){
        if (dominationCondition.dominates(x, y)){
          score += scores(y)+1
        }
      }
      result_scores(x) = score
      i+=1
    }
    val result = result_scores.map{case(k,v)=>Tuple2(k, v.toInt)}
    result.iterator
  }

  def sortByScore(iterator:Iterator[(List[Double], Int)]): Iterator[(List[Double], Int)] ={
    val array = iterator.toArray
    val sorted_array = array.sortBy(x => - x._2)
    sorted_array.toIterator
  }

  def addScoreAndCalculate(x: Iterator[List[Double]], scores: Map[List[Double], Int], k: Int): Iterator[(List[Double], Int)] ={
    val score = addScore(x, scores)
    val sortedScore = sortByScore(score)
    sortedScore.take(k)
  }

  def dominationScoreOfPoint(x: Iterator[List[Double]], points: List[List[Double]]): Iterator[(List[Double], Int)] = {
    var result: List[Int] = List()
    val x1 = x.toList
    for (point<-points) {
      val score = x1.count(p => dominationCondition.dominates(point, p))
      result = result :+ score
    }
    points.zip(result).iterator
  }
}

object dominationCondition extends Serializable {
  def dominates(a: List[Double], b:List[Double]): Boolean = {
    a.zip(b).forall(pair=>pair._1<=pair._2) && !a.toArray.deep.equals(b.toArray.deep)
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
    val upper = toBase(scala.math.pow(base, dims).toInt, base).length - 1
    val a1 = toBase(a, base).toArray.reverse.padTo(upper, 0).reverse
    val b1 = toBase(b, base).toArray.reverse.padTo(upper, 0).reverse
    val res = a1.zip(b1).forall(pair=>pair._1.toString.toDouble<pair._2.toString.toDouble) && !a1.deep.equals(b1.deep)
    res
  }
}

object NonDominatedPartitions extends Serializable {
  def calculate(partitions: Array[Int], divisionType: Int, dimensions: Int): Array[Int] = {
    var dominatedCells: Array[Int] = Array()
    for (c<-partitions){
      var flag = false
      for (c1<-partitions){
        if (dominationCondition.dominates(c1, c, divisionType, dimensions) && !flag) {
          dominatedCells = dominatedCells :+ c
          flag = true
        }
      }
    }
    partitions.toSet.diff(dominatedCells.toSet).toArray
  }
}

object Skyline {

  def main(args: Array[String]): Unit ={
    val sparkConf = new SparkConf().setMaster("local[8]").setAppName("Skyline Queries")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    val rdd = sc.textFile("uniform.csv",  40).map(x=>x.split(", ")).map(x => x.map(y => y.toDouble).toList)

    val divisionType = 5

    val grid = new CellGrid(rdd, divisionType)
    grid.makeGrid()

    val dimensions = rdd.take(1)(0).length

    val partitions = rdd.map(p => grid.findPartition(Row.fromSeq(p)))

    def removeDominatedPartitions(points: RDD[List[Double]], partitions: RDD[Int]): RDD[(Int, List[Double])] = {
      val nonEmptyPartitions = partitions.distinct().collect()
      val nonDominatedPartitions = NonDominatedPartitions.calculate(nonEmptyPartitions, divisionType, dimensions)
      val pointsWithPartition: RDD[(Int, List[Double])] = partitions.zip(points)
      val filteredPoints = pointsWithPartition.filter(p=>nonDominatedPartitions.contains(p._1))
      filteredPoints
    }

    def normalizePartitions(partitionsWithPoints: RDD[(Int, List[Double])]): RDD[(Int, List[Double])] = {
      val points = partitionsWithPoints.map(p=>p._2)
      val partitionsMap = partitionsWithPoints.map(p=>p._1).distinct().zipWithIndex().collectAsMap()
      val partitionsNormalized: RDD[Int] = partitionsWithPoints.map(_._1).map(p=>partitionsMap(p).toInt)
      partitionsNormalized.zip(points)
    }

    def topK(rdd: RDD[List[Double]], K: Int, grid_algo: Boolean, from_skyline: Boolean): List[List[Double]] = {
      @tailrec
      def loop(rdd1: RDD[List[Double]], k: Int, acc: List[List[Double]]): List[List[Double]]={
        if (k==0) acc
        else {
          if (grid_algo){
            val partitions = rdd1.map(p => grid.findPartition(Row.fromSeq(p)))
            val filteredPoints = normalizePartitions(partitions.zip(rdd1))
            val partitionsNormalized = filteredPoints.map(p=>p._1)
            val partitionedPoints = filteredPoints.partitionBy(new CustomPartitioner(partitionsNormalized.distinct().count().toInt)).map(p=>p._2)
            val skyline = partitionedPoints.repartition(1).mapPartitions(SFSkylineCalculation.calculate).collect().toList
            val partialScores = rdd1.mapPartitions(x => SFSkylineCalculation.dominationScoreOfPoint(x, skyline))
            val scores = partialScores.reduceByKey(_ + _).sortBy(-_._2)
            if (!from_skyline){
              val top = scores.take(1)(0)._1
              loop(rdd1.filter(_!=top), k-1, acc:+top)
            }
            else{
              val topk = scores.take(k).map(_._1).toList
              topk
            }
          }
          else {
            val rdd2 = rdd1.mapPartitions(SFSkylineCalculation.calculate)
            val skyline = rdd2.repartition(1).mapPartitions(SFSkylineCalculation.calculate).collect().toList
            val partialScores = rdd1.mapPartitions(x => SFSkylineCalculation.dominationScoreOfPoint(x, skyline))
            val scores = partialScores.reduceByKey(_ + _).sortBy(-_._2)
            if (!from_skyline){
              val top = scores.take(1)(0)._1
              loop(rdd1.filter(_!=top), k-1, acc:+top)
            }
            else{
              val topk = scores.take(k).map(_._1).toList
              topk
            }
          }
        }
      }
      loop(rdd, K, List[List[Double]]())
    }

    val TASK = 2

    if (TASK==1) {
      val t1 = System.nanoTime
      val rdd2 = rdd.mapPartitions(SFSkylineCalculation.calculate)
      val partialSkylinesALS = rdd2.collect()
      val skylineALS = sc.parallelize(partialSkylinesALS).repartition(1).mapPartitions(SFSkylineCalculation.calculate)
      println("Default partitioning: number of skyline points: "+skylineALS.count())
      val duration1 = (System.nanoTime - t1) / 1e9d
      println("Duration = " + duration1 + " seconds")
      //skylineALS.sortBy(p=>p.head).foreach(println)

      //skyline2.map(row => (row.toArray.mkString(" "))).saveAsTextFile("ALS")

      // This is for exluding dominated partitions from the calculation
      // Grid calculation
      val t2 = System.nanoTime
      val filteredPoints = normalizePartitions(removeDominatedPartitions(rdd, partitions))
      val partitionsNormalized = filteredPoints.map(p=>p._1)
      val partitionedPoints = filteredPoints.partitionBy(new CustomPartitioner(partitionsNormalized.distinct().count().toInt)).map(p=>p._2)
      val rdd3 = partitionedPoints.mapPartitions(SFSkylineCalculation.calculate)
      val partialSkylinesGrid = rdd3.collect()
      val skylineGrid = sc.parallelize(partialSkylinesGrid).repartition(1).mapPartitions(SFSkylineCalculation.calculate)
      println("Grid partitioning: number of skyline points: "+skylineGrid.count())
      val duration2 = (System.nanoTime - t2) / 1e9d
      println("Duration = " + duration2 + " seconds")
      //skylineGrid.sortBy(p=>p.head).foreach(println)

    }
    else if (TASK==2) {
      // Select top k with the best score
      val k = 3
      val t1 = System.nanoTime
      val domination_topk_ALS = topK(rdd, k, grid_algo = false, from_skyline = false)
      println("Default partitioning: top-"+k+" domination score points: ")
      domination_topk_ALS.sortBy(p=>p.head).foreach(println)
      val duration1 = (System.nanoTime - t1) / 1e9d
      println("Duration = " + duration1 + " seconds")

      //skyline2.map(row => (row.toArray.mkString(" "))).saveAsTextFile("ALS")

      val t2 = System.nanoTime
      val filteredPoints = normalizePartitions(partitions.zip(rdd))
      val partitionsNormalized = filteredPoints.map(p=>p._1)
      val partitionedPoints = filteredPoints.partitionBy(new CustomPartitioner(partitionsNormalized.distinct().count().toInt)).map(p=>p._2)

      val domination_topk_Grid = topK(partitionedPoints, k, grid_algo = true, from_skyline = false)
      println("Grid partitioning: top-"+k+" domination score points: ")
      domination_topk_Grid.sortBy(p=>p.head).foreach(println)
      val duration2 = (System.nanoTime - t2) / 1e9d
      println("Duration = " + duration2 + " seconds")

    }
    else if (TASK==3) {
      // Select top k with the best score
      val k = 3

      val t1 = System.nanoTime
      val domination_topk_ALS = topK(rdd, k, grid_algo = false, from_skyline = true)
      println("Default partitioning: top-"+k+" domination score points: ")
      domination_topk_ALS.sortBy(p=>p.head).foreach(println)
      val duration1 = (System.nanoTime - t1) / 1e9d
      println("Duration = " + duration1 + " seconds")

      val t2 = System.nanoTime
      val filteredPoints = normalizePartitions(partitions.zip(rdd))
      val partitionsNormalized = filteredPoints.map(p=>p._1)
      val partitionedPoints = filteredPoints.partitionBy(new CustomPartitioner(partitionsNormalized.distinct().count().toInt)).map(p=>p._2)

      val domination_topk_Grid = topK(partitionedPoints, k, grid_algo = true, from_skyline = true)
      println("Grid partitioning: top-"+k+" domination score points: ")
      domination_topk_Grid.sortBy(p=>p.head).foreach(println)
      val duration2 = (System.nanoTime - t2) / 1e9d
      println("Duration = " + duration2 + " seconds")
    }
    

    
    sc.stop()
  }
}
