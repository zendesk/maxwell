package com.zendesk.maxwellperf

import java.sql.{Connection, DriverManager}

import scala.concurrent.ExecutionContext.Implicits.global
import com.zendesk.maxwell.{BinlogPosition, Maxwell, MaxwellConfig}

import scala.concurrent.Future
import scala.io.Source
import scala.util.Try

class MaxwellPerf(args: Array[String]) {
  val config = new MaxwellConfig(args)

  val uri = config.replicationMysql.getConnectionURI(false)
  val connection = DriverManager.getConnection(uri, config.replicationMysql.user, config.replicationMysql.password)

  def exec_file(fname: String) = {
    val source = Source.fromFile(fname)
    var sql = ""
    for ( line <- source.getLines() ) {
      sql += line + "\n"
      if ( line.endsWith(";")) {
        Try {
          connection.createStatement.execute(sql.replaceFirst("INSERT INTO", "INSERT IGNORE INTO"))
        }.failed.map(println(_))
        sql = ""
      }
    }
  }

  def initMaxwell() = {
    var position: BinlogPosition = null
    val maxwell = new Maxwell(config) {
      override def onStart(): Unit = {
        position = this.getPosition
        this.terminate()
      }
    }

    maxwell.run()
    position
  }

  def loadData(schemaFile: String, dataFile: String) = {
    connection.createStatement.execute("drop database if exists test_maxwell_perf")
    connection.createStatement.execute("create database test_maxwell_perf")
    connection.setCatalog("test_maxwell_perf")

    println("creating database/schema ... ")
    exec_file(schemaFile)

    val position = initMaxwell()
    println("initial maxwell position: " + position)

    println("inserting data...")
    exec_file(dataFile)
  }

  def run() = {
    config.replayMode = true
    val maxwell = new Maxwell(config)
    new Thread {
      override def run(): Unit = {
        var list = List[Long]()
        val top = System.currentTimeMillis

        while (true) {
          val rows = maxwell.getRowsProduced
          list = (list :+ rows).takeRight(10)

          val elapsed = System.currentTimeMillis - top

          if ( elapsed > 0 ) {
            val movingAverage = (list.last - list.head) / list.size
            System.err.println(s"$rows in $elapsed ms -- ${rows / (elapsed.toFloat / 1000)} ($movingAverage moving average) rows/s")
            Thread.sleep(1000)
          }
        }
      }
    }.start()

    maxwell.run
  }
}


object MaxwellPerf extends App {
  if ( args.contains("--help") ) {
    println("usage: maxwell-perf --load schema_file data_file [maxwell_options... ]")
    println("     | maxwell-perf [maxwell_options... ]")
    System.exit(1)
  }

  if ( args.size > 0 && args(0) == "--load" ) {
    val schemaFile = args(1)
    val dataFile = args(2)

    val perf = new MaxwellPerf(args.takeRight(args.size - 3))
    perf.loadData(schemaFile, dataFile)
  } else {
    val perf = new MaxwellPerf(args)
    perf.run
  }
}

