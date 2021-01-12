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

object SFSSkylineCalculation extends Serializable {

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
      var score = 0
      for (y<-arr){
        if (dominationCondition.dominates(x, y)){
          if (scores(y)==0) score += 1
          else score += scores(y)
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
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Skyline Queries")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.textFile("gaussian2.csv").map(x=>x.split(", ")).map(x => x.map( y => y.toDouble).toList)

    val divisionType = 2

    val grid = new CellGrid(rdd, divisionType)
    grid.makeGrid()

    val dimensions = rdd.take(1)(0).length

    val partitions = rdd.map(p => grid.findPartition(Row.fromSeq(p)))

    val partitionedPoints = rdd.map(x=>(grid.findPartition(Row.fromSeq(x)), x)).
      partitionBy(new CustomPartitioner(partitions.distinct().count().toInt)).map(p=>p._2)
    
    val TASK = 1
    
    if (TASK==1) {
      val rdd2 = rdd.mapPartitions(SFSSkylineCalculation.calculate)
      val partialSkylinesALS = rdd2.collect()
      val skylineALS = sc.parallelize(partialSkylinesALS).repartition(1).mapPartitions(SFSSkylineCalculation.calculate)
      println("Default partitioning: number of skyline points: "+skylineALS.count())
      skylineALS.foreach(println)

      //skyline2.map(row => (row.toArray.mkString(" "))).saveAsTextFile("ALS")

      // This is for exluding dominated partitions from the calculation
      val pointsWithPartition = rdd.zip(partitions)
      val nonEmptyPartitions = pointsWithPartition.map(p=>p._2).distinct().collect()

      val nonDominatedPartitions = NonDominatedPartitions.calculate(nonEmptyPartitions, divisionType, dimensions)

      val filteredPoints = rdd.filter(p=>nonDominatedPartitions.contains(grid.findPartition(Row.fromSeq(p))))

      val partitionedFilteredPoints = filteredPoints.map(x=>(grid.findPartition(Row.fromSeq(x)), x)).
        partitionBy(new CustomPartitioner(nonDominatedPartitions.length)).map(p=>p._2)

      // Grid calculation
      val rdd3 = partitionedFilteredPoints.mapPartitions(SFSSkylineCalculation.calculate)
      val partialSkylinesGrid = rdd3.collect()
      val skylineGrid = sc.parallelize(partialSkylinesGrid).repartition(1).mapPartitions(SFSSkylineCalculation.calculate)
      println("Grid partitioning: number of skyline points: "+skylineGrid.count())
      skylineGrid.foreach(println)

    }
    else if (TASK==2) {
      // Select top k with the best score
      val k = 3

      val rdd2 = rdd.mapPartitions(x=>{
        val x1 = x.toArray
        val scores_init :Map[List[Double], Int] = x1.map(xs=>xs->0).toMap
        SFSSkylineCalculation.addScoreAndCalculate(x1.iterator, scores_init, k)
      })
      val partialResultsALS = rdd2.collect()
      //partialSkylines2.foreach(println)
      val domination_topk_ALS = sc.parallelize(partialResultsALS).repartition(1)
        .mapPartitions(x=>{
          val scores :Map[List[Double], Int] = partialResultsALS.map(p=>p._1->p._2).toMap
          SFSSkylineCalculation.addScoreAndCalculate(x.map(p=>p._1), scores, k)
        }).map(p=>p._1)
      println("Default partitioning: top-"+k+" domination score points: "+domination_topk_ALS.count())
      domination_topk_ALS.foreach(println)

      //skyline2.map(row => (row.toArray.mkString(" "))).saveAsTextFile("ALS")

      val rdd3 = partitionedPoints.mapPartitions(x=>{
        val x1 = x.toArray
        val scores_init :Map[List[Double], Int] = x1.map(xs=>xs->0).toMap
        SFSSkylineCalculation.addScoreAndCalculate(x1.iterator, scores_init, k)
      })
      val partialResultsGrid = rdd3.collect()
      //partialResultsGrid.foreach(println)
      val domination_top_k_Grid = sc.parallelize(partialResultsGrid).repartition(1).mapPartitions(x=>{
        val scores :Map[List[Double], Int] = partialResultsGrid.map(p=>p._1->p._2).toMap
        SFSSkylineCalculation.addScoreAndCalculate(x.map(p=>p._1), scores, k)
      }).map(p=>p._1)
      println("Grid partitioning: top-"+k+" domination score points: "+domination_top_k_Grid.count())
      domination_top_k_Grid.foreach(println)
      
    }
    else if (TASK==3) {
      
    }


    sc.stop()
  }
}
