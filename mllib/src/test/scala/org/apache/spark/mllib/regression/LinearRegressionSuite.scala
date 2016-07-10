/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.regression

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.{LocalClusterSparkContext, LinearDataGenerator,
  MLlibTestSparkContext}
import org.apache.spark.util.Utils
/**
 * 逻辑回归 和线性回归:都可以做预测,
 * 逻辑回归用在二值预测,比如预测一个客户是否会流失,只有0-不流失,1-流失；
 * 线性回归用来进行连续值预测,比如预测投入一定的营销费用时会带来多少收益
 */
private object LinearRegressionSuite {

  /** 3 features */
  val model = new LinearRegressionModel(weights = Vectors.dense(0.1, 0.2, 0.3), intercept = 0.5)
}

class LinearRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  def validatePrediction(predictions: Seq[Double], input: Seq[LabeledPoint]) {
    val numOffPredictions = predictions.zip(input).count { case (prediction, expected) =>
      // A prediction is off if the prediction is more than 0.5 away from expected value.
      math.abs(prediction - expected.label) > 0.5
    }
    // At least 80% of the predictions should be on.
    assert(numOffPredictions < input.length / 5)
  }

  // Test if we can correctly learn Y = 3 + 10*X1 + 10*X2
  test("linear regression") {
    val testRDD = sc.parallelize(LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 42), 2).cache()
      println(testRDD.collect().toVector)
      /**
       * Vector((11.214024772532593,[0.4551273600657362,0.36644694351969087]), 
       *        (-5.415689635517044,[-0.38256108933468047,-0.4458430198517267]), 
       *        (14.375642473813445,[0.33109790358914726,0.8067445293443565]), 
       *        (-3.912182009031524,[-0.2624341731773887,-0.44850386111659524]), 
       *        (8.068693902377857,[-0.07269284838169332,0.5658035575800715]), 
       *        (9.989306160850216,[0.8386555657374337,-0.1270180511534269]), 
       *        (5.758664490318773,[0.499812362510895,-0.22686625128130267]), 
       *        (-1.4121002968455942,[-0.6452430441812433,0.18869982177936828]), 
       *        (3.7251308576720352,[-0.5804648622673358,0.651931743775642]), 
       *        (-1.938409647234628,[-0.6555641246242951,0.17485476357259122]), 
       *        (9.624414753089715,[0.5025608135349202,0.14208069682973434]), 
       *        (9.690222138795097,[0.16004976900412138,0.505019897181302]), 
       *        (-9.263283117158242,[-0.9371635223468384,-0.2841601610457427]), 
       *        (7.505615612508772,[0.6355938616712786,-0.1646249064941625]), 
       *        (16.85753167804019,[0.9480713629917628,0.42681251564645817]), 
       *        (-1.6594646337530006,[-0.0388509668871313,-0.4166870051763918]), 
       *        (18.60320498664045,[0.8997202693189332,0.6409836467726933]), 
       *        (3.2079071580933616,[0.273289095712564,-0.26175701211620517]), 
       *        (-0.9655150963991648,[-0.2794902492677298,-0.1306778297187794]), 
       *        (1.8890755614772696,[-0.08536581111046115,-0.05462315824828923]), 
       *        (8.877797525536728,[-0.06195495876886281,0.6546448480299902]), 
       *        (2.626935941140799,[-0.6979368909424835,0.6677324708883314]), 
       *        (-2.025386041254353,[-0.07938725467767771,-0.43885601665437957]), 
       *        (-9.579487883768484,[-0.608071585153688,-0.6414531182501653]), 
       *        (10.012356086210719,[0.7313735926547045,-0.026818676347611925]), 
       *        (3.906945192698244,[-0.15805658673794265,0.26573958270655806]), 
       *        (3.2506277513176904,[0.3997172901343442,-0.3693430998846541]), 
       *        (1.5938956059471285,[0.14324061105995334,-0.25797542063247825]), 
       *        (16.581067225014348,[0.7436291919296774,0.6114618853239959]), 
       *        (2.6961802935212957,[0.2324273700703574,-0.25128128782199144]), 
       *        (14.945933338678383,[0.39449745853945895,0.817229160415142]), 
       *        (3.1135697479953257,[-0.6077058562362969,0.6182496334554788]), 
       *        (4.693864807073323,[0.2558665508269453,-0.07320145794330979]), 
       *        (-0.020569580660068545,[-0.38884168866510227,0.07981886851873865]), 
       *        (-1.9013903780326946,[0.27022202891277614,-0.7474843534024693]), 
       *        (-6.382088234075615,[-0.005346215048158909,-0.9453716674280683]), 
       *        (-6.59217734708216,[-0.9270309666195007,-0.032312290091389695]), 
       *        (4.0820629747657655,[0.31010676221964206,-0.20846743965751569]), 
       *        (9.36353089146331,[0.8803449313707621,-0.23077831216541722]), 
       *        (11.415444054382094,[0.29246395759528565,0.5409312755478819]), 
       *        (12.91870075204254,[0.7880855916368177,0.19767407429003536]), 
       *        (4.074739780561063,[0.9520689432368168,-0.845829774129496]), 
       *        (4.1177002260208315,[0.5502413918543512,-0.44235539500246457]), 
       *        (8.402616743196292,[0.7984106594591154,-0.2523277127589152]), 
       *        (-1.683979760711629,[-0.1373808897290778,-0.3353514432305029]), 
       *        (-1.921936041379436,[-0.3697050572653644,-0.11452811582755928]), 
       *        (-0.16151602816349428,[-0.807098168238352,0.4903066124307711]), 
       *        (2.4507980485186853,[-0.6582805242342049,0.6107814398427647]), 
       *        (-12.227205395823008,[-0.7204208094262783,-0.8141063661170889]), 
       *        (-5.4007269668845,[-0.9459402662357332,0.09666938346350307]), 
       *        (8.117672680666697,[-0.43560342773870375,0.9349906440170221]), 
       *        (7.961775958770297,[0.8090021580031235,-0.3121157071110545]), 
       *        (-0.5835907407691755,[-0.9718883630945336,0.6191882496201251]), 
       *        (10.238079296773607,[0.0429886073795116,0.670311110015402]), 
       *        (8.528566011825289,[0.16692329718223786,0.37649213869502973]), 
       *        (-3.6264695899042176,[0.11276440263810383,-0.7684997525607482]), 
       *        (12.761614825959546,[0.1770172737885798,0.7902845707138706]), 
       *        (2.9277859937589197,[0.2529503304079441,-0.23483801763662826]), 
       *        (17.788125954452543,[0.8072501895004851,0.6673992021927047]), 
       *        (7.489226068157748,[-0.4796127376677324,0.9244724404994455]), 
       *        (2.505603658421207,[-0.2049276879687938,0.1470694373531216]), 
       *        (4.563403819214299,[-0.48366999792166787,0.643491115907358]), 
       *        (8.572840033734433,[0.3183669486383729,0.22821350958477082]), 
       *        (-0.030117943889392457,[-0.023605251086149304,-0.2770587742156372]), 
       *        (14.88243510533112,[0.47596326458377436,0.7107229819632654]), 
       *        (5.02237042243666,[-0.3205057828114841,0.51605972926996]), 
       *        (7.729660010602799,[0.45215640988181516,0.01712446974606241]), 
       *        (6.039654064252453,[0.5508198371849293,-0.2478254241316491]), 
       *        (14.07446408002985,[0.7256483175955235,0.39418662792516]), 
       *        (2.177299638354621,[-0.6797384914236382,0.6001217520150142]), 
       *        (13.346171684304597,[0.4508991072414843,0.589749448443134]), 
       *        (16.45584898741092,[0.6464818311502738,0.7005669004769028]), 
       *        (5.470328873604419,[0.9699584106930381,-0.7417466269908464]), 
       *        (6.323748804404355,[0.22818964839784495,0.08574936236270037]), 
       *        (-3.331682099413653,[-0.6945765138377225,0.06915201979238828]), 
       *        (0.5357389381056457,[0.09798746565879424,-0.34288007110901964]), 
       *        (5.172137549492019,[0.440249350802451,-0.22440768392359534]), 
       *        (-14.72829349014839,[-0.9695067570891225,-0.7942032659310758]), 
       *        (-11.439280883483647,[-0.792286205517398,-0.6535487038528798]), 
       *        (9.402166574682662,[0.7952676470618951,-0.1622831617066689]), 
       *        (6.635763151919769,[0.6949189734965766,-0.32697929564739403]), 
       *        (-7.50592299041157,[-0.15359663581829275,-0.8951865090520432]), 
       *        (-1.5383895047092622,[0.2057889391931318,-0.6676656789571533]), 
       *        (4.238294736293792,[-0.03553655732400762,0.14550349954571096]), 
       *        (7.534508194863686,[0.034600542078191854,0.4223352065067103]), 
       *        (13.655407016972076,[0.35278245969741096,0.7022211035026023]), 
       *        (4.48947239211213,[0.5686638754605697,-0.4202155290448111]), 
       *        (0.6333546193122297,[-0.26102723928249216,0.010688215941416779]), 
       *        (8.215280780974554,[-0.4311544807877927,0.9500151672991208]), 
       *        (-3.1085652462448876,[0.14380635780710693,-0.7549354840975826]), 
       *        (2.5924007111568,[-0.13079299081883855,0.0983382230287082]), 
       *        (9.125245440502924,[0.15347083875928424,0.45507300685816965]), 
       *        (11.305667643232157,[0.1921083467305864,0.6361110540492223]), 
       *        (8.183770804242968,[0.7675261182370992,-0.2543488202081907]), 
       *        (12.706431848586465,[0.2927051050236915,0.680182444769418]), 
       *        (3.2986455066247977,[-0.8062832278617296,0.8266289890474885]), 
       *        (6.979381010458447,[0.22684501241708888,0.1726291966578266]), 
       *        (6.099186673441009,[-0.6778773666126594,0.9993906921393696]), 
       *        (10.465315437042944,[0.1789490173139363,0.5584053824232391]), 
       *        (-5.245508439024772,[0.03495894704368174,-0.8505720014852347]))
       */
    val linReg = new LinearRegressionWithSGD().setIntercept(true)
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(testRDD)
    assert(model.intercept >= 2.5 && model.intercept <= 3.5)//3.062070032411828

    val weights = model.weights //[9.735526941802604,9.700906954001237]
    assert(weights.size === 2)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(1) >= 9.0 && weights(1) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(
      3.0, Array(10.0, 10.0), 100, 17)  
    val validationRDD = sc.parallelize(validationData, 2).cache()
    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)
    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn Y = 10*X1 + 10*X2
  test("linear regression without intercept") {
    val testRDD = sc.parallelize(LinearDataGenerator.generateLinearInput(
      0.0, Array(10.0, 10.0), 100, 42), 2).cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(false)
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(testRDD)

    assert(model.intercept === 0.0)

    val weights = model.weights
    assert(weights.size === 2)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(1) >= 9.0 && weights(1) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(
      0.0, Array(10.0, 10.0), 100, 17)
    val validationRDD = sc.parallelize(validationData, 2).cache()

    // Test prediction on RDD.
    validatePrediction(model.predict(validationRDD.map(_.features)).collect(), validationData)

    // Test prediction on Array.
    validatePrediction(validationData.map(row => model.predict(row.features)), validationData)
  }

  // Test if we can correctly learn Y = 10*X1 + 10*X10000
  test("sparse linear regression without intercept") {
    val denseRDD = sc.parallelize(
      LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 42), 2)
    val sparseRDD = denseRDD.map { case LabeledPoint(label, v) =>
      val sv = Vectors.sparse(10000, Seq((0, v(0)), (9999, v(1))))
      LabeledPoint(label, sv)
    }.cache()
    val linReg = new LinearRegressionWithSGD().setIntercept(false)
    linReg.optimizer.setNumIterations(1000).setStepSize(1.0)

    val model = linReg.run(sparseRDD)

    assert(model.intercept === 0.0)

    val weights = model.weights
    assert(weights.size === 10000)
    assert(weights(0) >= 9.0 && weights(0) <= 11.0)
    assert(weights(9999) >= 9.0 && weights(9999) <= 11.0)

    val validationData = LinearDataGenerator.generateLinearInput(0.0, Array(10.0, 10.0), 100, 17)
    val sparseValidationData = validationData.map { case LabeledPoint(label, v) =>
      val sv = Vectors.sparse(10000, Seq((0, v(0)), (9999, v(1))))
      LabeledPoint(label, sv)
    }
    val sparseValidationRDD = sc.parallelize(sparseValidationData, 2)

      // Test prediction on RDD.
    validatePrediction(
      model.predict(sparseValidationRDD.map(_.features)).collect(), sparseValidationData)

    // Test prediction on Array.
    validatePrediction(
      sparseValidationData.map(row => model.predict(row.features)), sparseValidationData)
  }

  test("model save/load") {
    val model = LinearRegressionSuite.model

    val tempDir = Utils.createTempDir()
    val path = tempDir.toURI.toString

    // Save model, load it back, and compare.
    try {
      model.save(sc, path)
      val sameModel = LinearRegressionModel.load(sc, path)
      assert(model.weights == sameModel.weights)
      assert(model.intercept == sameModel.intercept)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}

class LinearRegressionClusterSuite extends SparkFunSuite with LocalClusterSparkContext {

  test("task size should be small in both training and prediction") {
    val m = 4
    val n = 200000
    val points = sc.parallelize(0 until m, 2).mapPartitionsWithIndex { (idx, iter) =>
      val random = new Random(idx)
      iter.map(i => LabeledPoint(1.0, Vectors.dense(Array.fill(n)(random.nextDouble()))))
    }.cache()
    // If we serialize data directly in the task closure, the size of the serialized task would be
    // greater than 1MB and hence Spark would throw an error.
    val model = LinearRegressionWithSGD.train(points, 2)
    val predictions = model.predict(points.map(_.features))
  }
}
