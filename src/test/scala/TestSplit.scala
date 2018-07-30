object TestSplit {
  def main(args: Array[String]): Unit = {
    val line = "hello world"
    val strings = line.split(" ")(0)
    println(strings.toList)

  }

}
