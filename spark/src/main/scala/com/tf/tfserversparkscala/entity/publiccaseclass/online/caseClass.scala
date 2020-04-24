package com.tf.tfserversparkscala.entity.publiccaseclass.online

import java.sql.Timestamp

/**
  * 此目录存放实时项目的case类
  *
  * @param name
  * @param age
  */
case class Student(name: String, age: Int)

case class Event(sessionId: String, timestamp: Timestamp)

