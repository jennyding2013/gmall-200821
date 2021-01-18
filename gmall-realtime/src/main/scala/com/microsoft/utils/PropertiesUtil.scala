package com.microsoft.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author Jenny.D
 * @create 2021-01-06 20:58
 */


object PropertiesUtil {
  def load(propertieName: String): Properties = {
    val prop: Properties = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().
      getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))

    prop
  }

}


