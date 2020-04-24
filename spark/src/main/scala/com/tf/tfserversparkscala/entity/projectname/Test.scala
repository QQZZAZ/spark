package com.tf.tfserversparkscala.entity.projectname


/**
  * 此目录根据不同项目存储非case类
  */
class Test(val name: String, val age: Int) {

  def getMethod() {
    println(name)
    println(age)
  }
}
