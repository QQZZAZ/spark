package com.tf.tfserversparkscala.common.tfserversparkscala.common.udfs


import com.tf.tfserversparkscala.utils.md5utils.MD5Utils
import org.apache.spark.sql.SparkSession

object SparkUDFs {
  def registerMd5Udfs(spark: SparkSession) = {
    spark.udf.register("MD5", (user_id: String) => {
      MD5Utils.secret(user_id, 8)
    })
  }
}
