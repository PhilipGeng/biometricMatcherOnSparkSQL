package TestCase.sparkSQL

/**
 * Created by philippy on 2016/3/30.
 */
object bioRun {
  def main(args:Array[String]): Unit ={
    val b = new biometrics()
    val t1 = b.run(10000)
    println(t1)
    val t2 = b.run(100000)
    println(t2)
    val t3 = b.run(1000000)
    println(t3)
    val t4 = b.run(10000000)
    println(t4)
    println(t1+" "+t2+" "+t3+" "+t4)
  }
}
