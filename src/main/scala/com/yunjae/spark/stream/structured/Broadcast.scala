package com.yunjae.spark.stream.structured

import java.io.{OutputStreamWriter, PrintWriter}
import java.net.ServerSocket

import scala.io.Source

object Broadcast {
  def main(args: Array[String]): Unit = {
    args match {
      case Array(port: String, fileName: String) => {
        val serverSocket = new ServerSocket(port.toInt)
        val socket = serverSocket.accept()
        val out: PrintWriter = new PrintWriter(
          new OutputStreamWriter(socket.getOutputStream)
        )

        for (line <- Source.fromFile(fileName).getLines()) {
          println(line.toString)
          out.println(line.toString)
          out.flush()
          Thread.sleep(1000)
        }

        socket.close()
      }
    }
  }
}
