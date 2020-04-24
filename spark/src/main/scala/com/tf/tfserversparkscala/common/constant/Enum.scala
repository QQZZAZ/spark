package com.tf.tfserversparkscala.common.constant

object EnumTest {

  /** *
    * 定义一个星期的枚举
    */
  object WeekDay extends Enumeration {
    type WeekDay = Value //声明枚举对外暴露的变量类型
    val Mon = Value("1")
    val Tue = Value("2")
    val Wed = Value("3")
    val Thu = Value("4")
    val Fri = Value("5")
    val Sat = Value("6")
    val Sun = Value("7")

    def checkExists(day: String) = this.values.exists(_.toString == day) //检测是否存在此枚举值
    def isWorkingDay(day: WeekDay) = !(day == Sat || day == Sun) //判断是否是工作日
    def showAll = this.values.foreach(println) // 打印所有的枚举值
  }

  def main(args: Array[String]): Unit = {

    println(WeekDay.checkExists("8")) //检测是否存在

    println(WeekDay.Sun == WeekDay.withName("7")) //正确的使用方法

    println(WeekDay.Sun == "7") //错误的使用方法

    WeekDay.showAll //打印所有的枚举值

    println(WeekDay.isWorkingDay(WeekDay.Sun)) //是否是工作日

  }

}