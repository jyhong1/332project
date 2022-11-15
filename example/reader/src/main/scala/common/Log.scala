package common

import org.apache.log4j.LogManager
import org.apache.log4j.Level

object Log{
  def methodName() = Thread.currentThread().getStackTrace()(3).getMethodName
  val log = LogManager.getLogger("Reader Example")
  def info(msg: => String= "")=  if(msg.length > 0)log.info(String.format("(%s) - %s", methodName(), msg)) else log.info("")
  def debug(msg: => String= "")= if(msg.length > 0) log.debug(String.format("(%s) - %s", methodName(), msg)) else log.debug("")
  def warn(msg: => String= "")= if(msg.length > 0) log.warn(String.format("(%s) - %s", methodName(), msg)) else log.warn("")
  def error(msg: => String= "")= if(msg.length > 0) log.error(String.format("(%s) - %s", methodName(), msg)) else log.error("")
  log.setLevel(Level.DEBUG)
}

