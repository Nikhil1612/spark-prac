package pack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.xerial.snappy.Snappy

object obj {
  case class schema(id:String,category:String,product:String)


  def main(args: Array[String]): Unit= {
    println("===started===")
    println
    val conf = new SparkConf().setAppName("wcfinal").setMaster ("local[*]").set("spark driver.host","localhost").set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf) // RDD
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder.config(conf).getOrCreate
    import spark.implicits._  //extra line for df conversion
    val df=spark.read.format("csv").option("header","true").load("file:///Users/nikhil/Downloads/data 2/df/df.csv")
    df.show()
    val df1=spark.read.format("csv").option("header","true").load("file:///Users/nikhil/Downloads/data 2/df/df1.csv")
    df1.show()
    val prod=spark.read.format("csv").option("header","true").load("file:///Users/nikhil/Downloads/data 2/df/prod.csv")
    prod.show()
    val cust=spark.read.format("csv").option("header","true").load("file:///Users/nikhil/Downloads/data 2/df/cust.csv")
    cust.show()
    df.createOrReplaceTempView("df")
    df1.createOrReplaceTempView("df1")
    prod.createOrReplaceTempView("prod")
    cust.createOrReplaceTempView("cust")
    spark.sql("select * from df order by id desc").show()
    spark.sql("select * from df where product is not null").show()
    spark.sql("select count(id) from df ").show()
   val parke=spark.read.format("parquet").load("file:///Users/nikhil/Downloads/data 2/part.parquet")
    parke.show()
    val orcd=spark.read.format("orc").load("file:///Users/nikhil/Downloads/data 2/data.orc")
    orcd.show()
    val jsn=spark.read.format("json").load("file:///Users/nikhil/Downloads/data 2/devices.json")
    jsn.show()
    val avva=spark.read.format("avro").load("file:///Users/nikhil/Downloads/data 2/data.avro")
    avva.show()
    avva.createOrReplaceTempView("writefile")
    val finaldf=spark.sql("select * from writefile")
    finaldf.write.format("csv").option("header","true").mode("overwrite").save("file:///Users/nikhil/Downloads/data/finaldf.csv")

    val da = spark. read. format ("csv"). option ("header", "true").load ("file:///Users/nikhil/Downloads/data 2/dt.txt")
    da.show()
    val selda = da. select("id", "category")
    selda. show()
    val dropda = da.drop ("product")
    dropda. show()
    val fild=da.filter(!(col("category") isin ("Exercise","Team Sports")))
    println("not filter")
    fild.show()
val filnul=da.filter(col("product") isNull)
    filnul.show()
    val filnotnul=da.filter(col("product") isNotNull)
    filnotnul.show()
    val nocash=da.filter(!(col("spendby")==="cash"))
    nocash.show()
    nocash.createOrReplaceTempView("addcol")
    spark.sql("select *,'zeyo' as status from addcol").show()
    nocash.selectExpr("id","tdate","amount","upper(category)").show()
  }
}
