// Imports
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.sql.Row
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/** This class manages the grid partitioner. It partitions an orthogonal space of d dimensions in equal interval cuts
 * in each dimension.
 * @param rdd The RDD on which the partitioner will be built
 * @param divisionType The amount of equal cuts per dimension
 */
class CellGrid(rdd: RDD[List[Double]], divisionType: Int) extends Serializable {

  var intervals: Array[Double] = Array()
  var bounds: List[List[Double]] = List()
  var dims: Int = 0

  /** This function converts `Any` values to doubles (values that really are doubles but for some reason are cast as Any)
   */
  def double(n: Any): Double = {
    n.toString.toDouble
  }

  /**
   * This function creates the grid partitions boundaries and the cut intervals
   */
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

  /**
   * Find the partition Id of a point represented with its coordinates
   * @param number The point in coordinates
   * @return The partition ID of the point
   */
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

/**
 * Class to manage the Spark partitioning
 * @param numOfPartitions How many partitions there will be
 */
class CustomPartitioner(numOfPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  override def numPartitions: Int = numOfPartitions
}

/**
 * This object handles the skyline actions.
 */
object SFSkylineCalculation extends Serializable {
  /**
   * THis function calculates the skyline for an iterator that expresses a partition.
   * @param a The iterator input
   * @return The local skyline.
   */
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

  /**
   * This method calculates the domination score of a list of points on an iterator representing a partition.
   * @param x The partition data iterator
   * @param points The list of points for which the domination score will be calculated.
   * @return The points zipped with their domination score.
   */
  def dominationScoreOfPoints(x: Iterator[List[Double]], points: List[List[Double]]): Iterator[(List[Double], Int)] = {
    var result: List[Int] = List()
    val x1 = x.toList
    for (point<-points) {
      val score = x1.count(p => dominationCondition.dominates(point, p))
      result = result :+ score
    }
    points.zip(result).iterator
  }
}

/**
 * This object handles the domination condition for points and partitions.
 */
object dominationCondition extends Serializable {
  /**
   * This function returns if the first input dominates the second input.
   * @param a The first input point
   * @param b The second input point
   * @return If a dominates b
   */
  def dominates(a: List[Double], b:List[Double]): Boolean = {
    a.zip(b).forall(pair=>pair._1<=pair._2) && !a.toArray.deep.equals(b.toArray.deep)
  }

  /**
   * Convert a decimal number n to base b
   * @param n The decimal number
   * @param b The base
   * @return n expressed in base b (digits are elements of a list)
   */
  def toBase(n: Int, b: Int): List[Double] ={
    @tailrec
    def loop(acc: List[Double], n: Int): List[Double]={
      if (n==0) acc
      else loop(acc:+(n%b).toDouble, n/b)
    }
    loop(List(), n).reverse
  }

  /**
   * This function returns if the first input (partition ID) dominates the second input (also partition ID). Applies only to grid partitioning.
   * @param a The first input
   * @param b The second input
   * @param base How many intervals per dimension
   * @param dims How many dimensions
   * @return If partition a dominates b
   */
  def dominates(a: Int, b:Int, base: Int, dims: Int): Boolean ={
    val upper = toBase(scala.math.pow(base, dims).toInt, base).length - 1
    // Partition numbers are converted to their representation as base b
    // These representations serve as a signature for the partition
    // and are used to calculate the dominance
    val a1 = toBase(a, base).toArray.reverse.padTo(upper, 0).reverse
    val b1 = toBase(b, base).toArray.reverse.padTo(upper, 0).reverse
    val res = a1.zip(b1).forall(pair=>pair._1.toString.toDouble<pair._2.toString.toDouble) && !a1.deep.equals(b1.deep)
    res
  }
}

/**
 * This object handles the calculation of non-dominated partitions for grid partitioning
 */
object NonDominatedPartitions extends Serializable {
  /**
   * This function calculates the partitions that are not dominated by any other partition
   * @param partitions The partitions array
   * @param divisionType How many intervals per dimension
   * @param dimensions How many dimensions
   * @return The non-dominated partitions
   */
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

    // Loading the file
    val rdd = sc.textFile("anticorrelated.csv",  40).map(x=>x.split(", ")).map(x => x.map(y => y.toDouble).toList)

    // Number of intervals for each dimension
    val divisionType = 5

    // The grid partitioner
    val grid = new CellGrid(rdd, divisionType)
    grid.makeGrid()

    val dimensions = rdd.take(1)(0).length

    // RDD for the partition IDs for each point
    val partitions = rdd.map(p => grid.findPartition(Row.fromSeq(p)))

    /**
     * This function removes points that are in dominated partitions from the points RDD (Grid)
     * @param points the points RDD
     * @param partitions the partition IDs RDD
     * @return The points in non-dominated partitions
     */
    def removeDominatedPartitions(points: RDD[List[Double]], partitions: RDD[Int]): RDD[(Int, List[Double])] = {
      val nonEmptyPartitions = partitions.distinct().collect()
      val nonDominatedPartitions = NonDominatedPartitions.calculate(nonEmptyPartitions, divisionType, dimensions)
      val pointsWithPartition: RDD[(Int, List[Double])] = partitions.zip(points)
      val filteredPoints = pointsWithPartition.filter(p=>nonDominatedPartitions.contains(p._1))
      filteredPoints
    }

    /**
     * This function converts the partition IDs of points to a continuous array, i.e. if there are 20 partitions in total
     * they are all converted to 0, 1, 2, ..., 19. (Grid)
     * @param partitionsWithPoints The partition-point RDD
     * @return The partition-point RDD with processed partition IDs
     */
    def normalizePartitions(partitionsWithPoints: RDD[(Int, List[Double])]): RDD[(Int, List[Double])] = {
      val points = partitionsWithPoints.map(p=>p._2)
      val partitionsMap = partitionsWithPoints.map(p=>p._1).distinct().zipWithIndex().collectAsMap()
      val partitionsNormalized: RDD[Int] = partitionsWithPoints.map(_._1).map(p=>partitionsMap(p).toInt)
      partitionsNormalized.zip(points)
    }

    /**
     * This function implements the STD algorithm for finding the topk elements of a set based on a scoring function.
     * Here, it finds the topk with the highest domination score.
     * @param rdd The points RDD
     * @param K The k parameter
     * @param grid_algo If the Grid partitioning approach is used (if not, then ALS is used)
     * @param from_skyline If the topk points are to be extracted from the skyline of the initial RDD
     * @return The topk points
     */
    def topK(rdd: RDD[List[Double]], K: Int, grid_algo: Boolean, from_skyline: Boolean): List[List[Double]] = {
      /**
       * Loop helper function for calculating the topk
       * @param rdd1 The temporary point RDD
       * @param k The k parameter
       * @param acc The accumulator
       * @return THe topk points
       */
      @tailrec
      def loop(rdd1: RDD[List[Double]], k: Int, acc: List[List[Double]]): List[List[Double]]={
        if (k==0) acc // base case
        else {
          if (grid_algo){ // if the grid partitioner is used
            val partitions = rdd1.map(p => grid.findPartition(Row.fromSeq(p)))
            val filteredPoints = normalizePartitions(partitions.zip(rdd1))
            val partitionsNormalized = filteredPoints.map(p=>p._1)
            val partitionedPoints = filteredPoints.partitionBy(new CustomPartitioner(partitionsNormalized.distinct().count().toInt)).map(p=>p._2)
            val skyline = partitionedPoints.repartition(1).mapPartitions(SFSkylineCalculation.calculate).collect().toList
            val partialScores = rdd1.mapPartitions(x => SFSkylineCalculation.dominationScoreOfPoints(x, skyline))
            val scores = partialScores.reduceByKey(_ + _).sortBy(-_._2)
            if (!from_skyline){ // if we are not asking for topk to be extracted from skyline
              val top = scores.take(1)(0)._1
              loop(rdd1.filter(_!=top), k-1, acc:+top) // then run again
            }
            else{
              val topk = scores.take(k).map(_._1).toList // else take topk from skyline
              topk
            }
          }
          else { // similar for ALS
            val rdd2 = rdd1.mapPartitions(SFSkylineCalculation.calculate)
            val skyline = rdd2.repartition(1).mapPartitions(SFSkylineCalculation.calculate).collect().toList
            val partialScores = rdd1.mapPartitions(x => SFSkylineCalculation.dominationScoreOfPoints(x, skyline))
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

    // select task
    val TASK = 1

    if (TASK==1) {
      // ALS calculation
      val t1 = System.nanoTime
      val rdd2 = rdd.mapPartitions(SFSkylineCalculation.calculate)
      val partialSkylinesALS = rdd2.collect()
      val skylineALS = sc.parallelize(partialSkylinesALS).repartition(1).mapPartitions(SFSkylineCalculation.calculate)
      println("Default partitioning: number of skyline points: "+skylineALS.count())
      val duration1 = (System.nanoTime - t1) / 1e9d
      println("Duration = " + duration1 + " seconds")
      skylineALS.sortBy(p=>p.head).foreach(println)

      // Grid calculation
      val t2 = System.nanoTime
      // This is for exluding dominated partitions from the calculation
      val filteredPoints = normalizePartitions(removeDominatedPartitions(rdd, partitions))
      val partitionsNormalized = filteredPoints.map(p=>p._1)
      // partitioning by the partition ID
      val partitionedPoints = filteredPoints.partitionBy(new CustomPartitioner(partitionsNormalized.distinct().count().toInt)).map(p=>p._2)
      val rdd3 = partitionedPoints.mapPartitions(SFSkylineCalculation.calculate)
      val partialSkylinesGrid = rdd3.collect()
      val skylineGrid = sc.parallelize(partialSkylinesGrid).repartition(1).mapPartitions(SFSkylineCalculation.calculate)
      println("Grid partitioning: number of skyline points: "+skylineGrid.count())
      val duration2 = (System.nanoTime - t2) / 1e9d
      println("Duration = " + duration2 + " seconds")
      skylineGrid.sortBy(p=>p.head).foreach(println)

    }
    else if (TASK==2) {
      // Select top k with the best score
      val k = 3
      // ALS calculation
      val t1 = System.nanoTime
      val domination_topk_ALS = topK(rdd, k, grid_algo = false, from_skyline = false)
      println("Default partitioning: top-"+k+" domination score points: ")
      domination_topk_ALS.sortBy(p=>p.head).foreach(println)
      val duration1 = (System.nanoTime - t1) / 1e9d
      println("Duration = " + duration1 + " seconds")

      // Grid calculation
      val t2 = System.nanoTime
      val filteredPoints = normalizePartitions(partitions.zip(rdd))
      val partitionsNormalized = filteredPoints.map(p=>p._1)
      // partitioning by the partition ID
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

      // ALS calculation
      val t1 = System.nanoTime
      val domination_topk_ALS = topK(rdd, k, grid_algo = false, from_skyline = true)
      println("Default partitioning: top-"+k+" domination score points: ")
      domination_topk_ALS.sortBy(p=>p.head).foreach(println)
      val duration1 = (System.nanoTime - t1) / 1e9d
      println("Duration = " + duration1 + " seconds")

      // Grid calculation
      val t2 = System.nanoTime
      val filteredPoints = normalizePartitions(partitions.zip(rdd))
      val partitionsNormalized = filteredPoints.map(p=>p._1)
      // partitioning by the partition ID
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
