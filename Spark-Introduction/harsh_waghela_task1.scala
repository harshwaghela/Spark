import org.apache.spark.{SparkConf,SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import java.io._
import org.json4s.jackson.Serialization




object harsh_waghela_task1 {

    def toList(tuple: Product): List[String] = { tuple.productIterator.map(_.toString).toList }

    case class Output(
                       n_review:Long,
                       n_review_2018:Long,
                       n_user: Long,
                       top10_user: List[List[String]],
                       n_business:Long,
                       top10_business: List[List[String]]
                     )

    def main(args: Array[String]): Unit = {

        val input_file= args(0)
        val output_file=args(1)
        implicit val formats = org.json4s.DefaultFormats
        val conf= new SparkConf().setAppName("task1").setMaster("local[*]")
        val sc = new SparkContext(conf)
        val rdd = sc.textFile(input_file)
        val rdd1 = rdd.map( line => parse(line).values.asInstanceOf[Map[String, Any]])
        val rdd2 = rdd1.map(line => line("review_id"))
        val total_count= rdd2.count()
        val no_2018= rdd1.map( x => (x("date").toString.split('-')(0),1)).filter( _._1 =="2018").count()
        val distinct_users= rdd1.map(line=> line("user_id")).distinct().count()
        val distinct_business=rdd1.map(line=> line("business_id")).distinct().count()

        val top10_users= rdd1.map( x => (x("user_id").toString(),1)).reduceByKey((x,y)=>x+y).sortByKey().takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))
        val top10_users_list = top10_users.map(x => toList(x))
        val top10_business= rdd1.map( x => (x("business_id").toString(),1)).reduceByKey((x,y)=>x+y).sortByKey().takeOrdered(10)(Ordering[Int].reverse.on(x=>x._2))
        val top10_business_list = top10_business.map(x => toList(x))

        val o = Output(total_count,no_2018,distinct_users,top10_users_list.toList,distinct_business,top10_business_list.toList)
        val json = Serialization.write(o)
        val pw = new PrintWriter(new File(output_file ))
        pw.write(json)
        pw.close





    }


}
