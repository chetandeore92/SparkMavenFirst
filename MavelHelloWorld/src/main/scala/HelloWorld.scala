import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

//scala - 2.12.15
//spark - 3.2.0
//java - 11
object HelloWorld {
  def main(str : Array[String]): Unit =
  {
    val sc = new SparkContext("Campaindata")

    val file = sc.textFile("/Users/chetandeore/Documents/Study/BigData/Spark/SparkDatasets1/bigdatacampaigndata-201014-183159.csv")

    file.persist(StorageLevel.MEMORY_AND_DISK_2)

    //file.collect().foreach(println)

    val filteredFile = file.map(x => {
      val splitFile = x.split(",")
      (splitFile(0),splitFile(10).toFloat)
    })

    //filteredFile.take(20).foreach(println)

    val wordsPrices = filteredFile.flatMap(x => {
      val arrayOfWords = x._1.split(" ")
      var mapWithPrices = new ListBuffer[(String,Float)]
      val averagePricePerWord = x._2 / arrayOfWords.length
      for(word <- arrayOfWords)
      {
        mapWithPrices.append((word,averagePricePerWord))
      }
      mapWithPrices
    })

    val ammountSpentPerWord = wordsPrices.reduceByKey((x,y)=> {
      x+y
    })

    ammountSpentPerWord.sortBy(x => x._2,false).collect().foreach(println)

    scala.io.StdIn.readLine()
  }

}
