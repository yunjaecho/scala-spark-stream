package com.yunjae.spark.stream.strandalone

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

class MeetupReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  val host = "stream.meetup.com"
  val port = 80

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run(): Unit = {receive()}
    }.start()
  }

  override def onStop(): Unit = {}

  private def receive(): Unit = {
   try {
     val socket = new Socket(host, port)

     val headers =
       s"""GET /2/rsvps HTTP/1.1
          |Host: dic.daum.net
          |Connection: keep-alive
        """.stripMargin

     val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream)))
     out.println(headers)
     out.flush()

     val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))

     var userInput: String = null
     userInput = reader.readLine()
     while(!isStopped && userInput != null) {
       store(userInput)
       userInput = reader.readLine()
     }
     reader.close()
     socket.close()

     restart("Trying to connect again")
   } catch {
     case e: java.net.ConnectException =>
       restart("Error connecting to " + host + ":" + port , e)
     case t: Throwable =>
       restart("Error receiving data", t)
   }
  }
}
