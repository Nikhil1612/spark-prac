package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object obj {


   case class columns(id:String, category: String,product: String)
    def main(args: Array[String]): Unit= {
    println("===started===")
    println
    val conf = new SparkConf().setAppName("wcfinal").setMaster ("local[*]").set("spark driver.host","localhost").set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf) // RDD
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(conf).getOrCreate //Dataframe
    val data = sc. textFile("file:///Users/nikhil/Downloads/data/zf.txt")
    println
     data.foreach(println)
     println
    val split = data.map( x => x.split(","))

    val schemardd = split.map( x => columns(x(0),x(1),x(2)))
    val filrdd = schemardd filter (x => x. product.contains("a"))
    println
    println
    filrdd.foreach(println)


  }
}