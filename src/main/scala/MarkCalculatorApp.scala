import org.apache.spark.sql.{Encoder, Encoders}

object MarkCalculatorApp {

  import org.apache.spark.sql.SparkSession
  case class StudentMark(id: Int, sub1: Int, sub2: Int, sub3: Int, sub4: Int, sub5: Int, sub6: Int, total: Float, percentage: Float, semester: Int, result: String)
  case class Student(id: Int, name: String)
    def main(args: Array[String]) {
      implicit val StudentMarkEncoder: Encoder[StudentMark] = Encoders.product[StudentMark]
      implicit val StudentEncoder: Encoder[Student] = Encoders.product[Student]


      val studentMarkFile = "src/main/resources/StudentMarks.txt"
      val studentNameFile = "src/main/resources/StudentRollNo.txt"
      val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local")
        .getOrCreate()
      import spark.implicits._
      val studentMarksDF = spark.read.textFile(studentMarkFile).map(_.split("\\s+")).map{ attributes =>
        val marks = List(attributes(1).toInt, attributes(2).toInt, attributes(3).toInt, attributes(4).toInt, attributes(5).toInt, attributes(6).toInt)
        val total: Float = attributes(1).toInt + attributes(2).toInt +  attributes(3).toInt + attributes(4).toInt + attributes(5).toInt +  attributes(6).toInt
        val percentage: Float = (total / 600) * 100
        val pass: String = if(marks.exists(x => x < 40)) "F" else  "P"
        StudentMark(attributes(0).toInt, attributes(1).toInt, attributes(2).toInt, attributes(3).toInt, attributes(4).toInt, attributes(5).toInt, attributes(6).toInt, total, percentage, attributes(7).toInt, pass)
      }.toDF()
      val studentNameDF = spark.read.textFile(studentNameFile).map(_.split("\\s+")).map(s => Student(s(0).toInt, s(1))).toDF()
      studentNameDF.createOrReplaceTempView("Students")
      studentMarksDF.createOrReplaceTempView("StudentMarks")
      val response = spark.sql("SELECT stdm.id, std.name total, percentage, semester, result FROM StudentMarks as stdm, Students AS std WHERE std.id = stdm.id  ORDER BY semester ASC, total DESC ")
      response.toJavaRDD.coalesce(1,false).saveAsTextFile("src/main/resources/output.txt")
      spark.stop()
    }

}
