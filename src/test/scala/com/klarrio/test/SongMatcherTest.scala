package com.klarrio.test

import com.github.vickumar1981.stringdistance.StringDistance.NGram
import com.klarrio.SongMatcher
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SongMatcherTest extends  AnyWordSpec with Matchers {


  "run the match method to output match result in files/output"  in {

    SongMatcher.clearResultFromPrevoiseRun()
    val spark = SparkSession.builder()
      .appName("SongMatcher")
      .master("local[*]")
      .getOrCreate()

    SongMatcher.matchSongs(spark)

    val exactMatchDF = SongMatcher.loadFile(spark, "files/output/titleAndLookupKeyExactMatch")
    val fuzzyMatchDF = SongMatcher.loadFile(spark, "files/output/titleAndLookupKeyAndWriterFuzzyMatch")
    val numberOfExactMatch = exactMatchDF.count()
    val numberOfFuzzyMatch = fuzzyMatchDF.count()
    fuzzyMatchDF.createOrReplaceTempView("fuzzy")
    val duplicateInFuzzySql = "select sum(count) x from " +
      "( select fuzzy.unique_input_record_ , count(*) count from fuzzy group by fuzzy.unique_input_record_ )  " +
      "where count > 1 "
    val duplicateInFuzzyDF = spark.sql(duplicateInFuzzySql)
    val numberOfDuplicatedInFuzzyMatch = duplicateInFuzzyDF.first().getLong(0)
    println (s"numberOfExactMatch: $numberOfExactMatch")
    println (s"numberOfFuzzyMatch: $numberOfFuzzyMatch")
    println (s"numberOfDuplicatedInFuzzyMatch: $numberOfDuplicatedInFuzzyMatch")

  }


  "test fuzzy match algorithm"  in {
    val ls = List(("Guy Gerber,Shlomi Aber ", "ABERGEL S/GERBER G"),
      ("Tomaso Albinoni", "Tomaso"),
      ("A", "A"),
      ("Aaaaaaaaaa", "Aaaaaaaaaaaaa"),
      ("Andy", "andrew"),
      ("Andy", "andy"),
      ("ADDDD", "UYDD"),
      ("ADDDD", "UYHN"))

    ls.foreach(t => {
      //        val m =  SongMatcher.stringFuzzyMatch.apply(t._1, t._2)
      val m =  NGram.score(t._1, t._2, 4)
      println(s"score for $t is: $m")
    })

  }

  "test title to lookup key algorithm" in {

    val ls = List("Jekyll & Hyde                                                                                       ",
      "Tibetan Singing Bowls                                                                               ",
      "Medizinmann                                                                                         ",
      "Keyboard Suite No. 8 in F Minor, HWV 433: III. Allemande                                            ",
      "Jackie Jackie (Spend This Winter With Me)                                                           ",
      "Short Short Twist                                                                                   ",
      "Silhouette                                                                                          ",
      "Woman's World                                                                                       ",
      "Apneia                                                                                              ",
      "BondiÃ©                                                                                             ",
      "Ghetto Starz                                                                                        ",
      "Do My Thing                                                                                         ",
      "Break Up To Make Up                                                                                 ",
      "Il santo morto                                                                                      ",
      "Singing Om                                                                                          ",
      "Vapen till dom hopplÃ¶sa                                                                            ",
      "N'importe quel gars                                                                                 ",
      "Romeo And Juliet (Love Theme)                                                                       ",
      "Rain Over Kilimantscharo                                                                            ",
      "Ain't Got Time                                                                                      ",
      "Bad Spirits (Bani)                                                                                  ",
      "Velle                                                                                               ",
      "Tooth Fairy                                                                                         ",
      "Un Uomo Diverso             ")

    val  ls2 =  ls.map(x =>   SongMatcher.titleToLookupKey(x))

    ls2 shouldBe List("JEKYLL&HYDE              ",
      "TIBETANSINGINGBOWLS      ",
      "MEDIZINMANN              ",
      "KEYBOARDSUITENO8INFMINORH",
      "JACKIEJACKIESPENDTHISWINT",
      "SHORTSHORTTWIST          ",
      "SILHOUETTE               ",
      "WOMANSWORLD              ",
      "APNEIA                   ",
      "BONDI                    ",
      "GHETTOSTARZ              ",
      "DOMYTHING                ",
      "BREAKUPTOMAKEUP          ",
      "ILSANTOMORTO             ",
      "SINGINGOM                ",
      "VAPENTILLDOMHOPPLSA      ",
      "NIMPORTEQUELGARS         ",
      "ROMEOANDJULIETLOVETHEME  ",
      "RAINOVERKILIMANTSCHARO   ",
      "AINTGOTTIME              ",
      "BADSPIRITSBANI           ",
      "VELLE                    ",
      "TOOTHFAIRY               ",
      "UNUOMODIVERSO            ")
  }
}
