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
        val subject1 = attributes(1).toInt
        val subject2 = attributes(2).toInt
        val subject3 = attributes(3).toInt
        val subject4 = attributes(4).toInt
        val subject5 = attributes(5).toInt
        val subject6 = attributes(6).toInt
        val marks = List(subject1, subject2, subject3, subject4, subject5, subject6)
        val total: Float = marks.sum
        val percentage: Float = (total / 600) * 100
        val pass: String = if(marks.exists(x => x < 40)) "F" else  "P"
        StudentMark(attributes(0).toInt, subject1, subject2, subject3, subject4, subject5, subject6, total, percentage, attributes(7).toInt, pass)
      }.toDF()
      val studentNameDF = spark.read.textFile(studentNameFile).map(_.split("\\s+")).map(s => Student(s(0).toInt, s(1))).toDF()
      studentNameDF.createOrReplaceTempView("Students")
      studentMarksDF.createOrReplaceTempView("StudentMarks")
      val response = spark.sql("SELECT std.id, std.name, COALESCE(total,0), COALESCE(percentage,0), semester, COALESCE(result, 'F') FROM StudentMarks as stdm RIGHT JOIN Students AS std ON std.id = stdm.id  ORDER BY semester ASC, total DESC ")
      response.toJavaRDD.coalesce(1,false).saveAsTextFile("src/main/resources/output.txt")
      spark.stop()
    }

}
