package com.test.spark.wiki.extracts

import java.io.FileReader
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.select.Elements
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)

import session.implicits._

      // TODO Q1 Transformer cette seq en dataset
    getLeagues
      .toDS()
      .flatMap { input =>
        (fromDate.getYear until toDate.getYear).map(
          year =>
            year + 1 -> (input.name, input.url.format(year, year + 1))
        )
      }.flatMap {
      case (season, (league, url)) =>
        try {


          Document doc = Jsoup.connect(url).get()
          Elements elementlists = doc.select("table[class=wikitable gauche]")
          val tab = elementlists.select("tr[bgcolor=\"white\"]"
            LeagueStanding(league, season, tab[0].text().toInt, tab[1].text().toString, tab[2].text().toInt, tab[3].text(), tab[4].text(), tab[5].text(),
            tab[6].text(), tab[7].text(), tab[8].text(), tab[9].text())


        } catch {
          case _: Throwable =>
            // TODO Q3 En supposant que ce job tourne sur un cluster EMR, où seront affichés les logs d'erreurs ?
            logger.warn(s"Can't parse season $season from $url")
            Seq.empty
        }
    })
      // TODO Q4 Comment partitionner les données en 2 avant l'écriture sur le bucket
      .repartition(2)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(bucket)

    // TODO Q5 Quel est l'avantage du format parquet par rapport aux autres formats ?
// Parquet supporte plusieurs moteurs d'exécution, permet une structure de données plus complexe et l'évolution du schema de la données

    // TODO Q6 Quel est l'avantage de passer d'une séquence scala à un dataset spark ?
    //L'avantage est de profiter du parallélisme lors du traitement des données
  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    // TODO Q7 Recuperer l'input stream du fichier leagues.yaml
    val inputStream= new FileReader("leagues.yaml")
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq
  }
}

// TODO Q8 Ajouter les annotations manquantes pour pouvoir mapper le fichier yaml à cette classe
case class LeagueInput(@JsonProperty("name") name: String,
                       @JsonProperty("url") url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
