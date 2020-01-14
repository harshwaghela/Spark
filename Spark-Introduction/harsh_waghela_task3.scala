import org.apache.spark.{SparkConf,SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import java.io._
import org.json4s.jackson.Serialization

object harsh_waghela_task3 {

  case class Output(
                     m1:Float,
                     m2:Float,
                     explanation: String
                   )


  def main(args: Array[String]): Unit = {
    val input_file1= args(0)
    val input_file2=args(1)
    val output_file1=args(2)
    val output_file2=args(3)
    implicit val formats = org.json4s.DefaultFormats
    val conf= new SparkConf().setAppName("task3").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile(input_file1).map( line => parse(line).values.asInstanceOf[Map[String, Any]])
    val rdd2 = sc.textFile(input_file2).map( line => parse(line).values.asInstanceOf[Map[String, Any]])
    val rdd_review= rdd1.map(line => (line("business_id").toString,line("stars").toString))
    val rdd_business = rdd2.map(line => (line("business_id").toString,line("city").toString))
    val joined = rdd_business.join(rdd_review).map(x => ((x._2._1),(x._2._2.toFloat,1))).reduceByKey((x,y)=>(x._1+y._1,y._2+x._2)).map(x => (x._1,x._2._1/x._2._2)).sortByKey().sortBy(x=>x._2,false)
    val time_collect = System.currentTimeMillis()
    val coll = joined.collect()
    var i: Int=0
    while(i < 10){
      print(coll(i))
      i+=1
    }
    var end_collect=System.currentTimeMillis()
    val m1 = end_collect - time_collect
    var out="city,stars\n"
    for(entry <- coll){
      out+= entry._1 +","+entry._2 +"\n"
    }

    val time_take = System.currentTimeMillis()
    val tak = joined.take(10)
    tak.foreach(print)
    val end_take  = System.currentTimeMillis()
    val m2= end_take - time_take
    val expl:String= "The time taken by the take action while running locally on my system was very low compared to using the collect action. This is mainly due to the lazy nature of take that only necessary values get calculated during the previous sort operation whereas all the data is returned for collect action causing it to take longer time."


    val pw1 = new PrintWriter(new File(output_file1))
    pw1.write(out)
    pw1.close

    val o = Output(m1/1000,m2/1000,expl)
    val json = Serialization.write(o)
    val pw = new PrintWriter(new File(output_file2))
    pw.write(json)
    pw.close






  }
}
