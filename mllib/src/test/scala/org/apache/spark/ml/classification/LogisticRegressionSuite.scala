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

package org.apache.spark.ml.classification

import scala.annotation.varargs
import scala.reflect.runtime.universe

import org.apache.spark.SparkFunSuite
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.MLTestingUtils
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateMultinomialLogisticInput
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils.DoubleWithAlmostEquals
import org.apache.spark.mllib.util.TestingUtils.VectorWithAlmostEquals
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.SQLUserDefinedType

class LogisticRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _
  @transient var binaryDataset: DataFrame = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()

     dataset = sqlContext.createDataFrame(generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42))

    /*
       Here is the instruction describing how to export the test data into CSV format
       so we can validate the training accuracy compared with R's glmnet package.

       import org.apache.spark.mllib.classification.LogisticRegressionSuite
       val nPoints = 10000
       val weights = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
       val xMean = Array(5.843, 3.057, 3.758, 1.199)
       val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)
       val data = sc.parallelize(LogisticRegressionSuite.generateMultinomialLogisticInput(
         weights, xMean, xVariance, true, nPoints, 42), 1)
       data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1) + ", "
         + x.features(2) + ", " + x.features(3)).saveAsTextFile("path")
     */
    binaryDataset = {
      val nPoints = 10000
      val weights = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData = generateMultinomialLogisticInput(weights, xMean, xVariance, true, nPoints, 42)

      sqlContext.createDataFrame(
        generateMultinomialLogisticInput(weights, xMean, xVariance, true, nPoints, 42))
    }
  }

  test("params") {//参数
    ParamsSuite.checkParams(new LogisticRegression)
    val model = new LogisticRegressionModel("logReg", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("logistic regression: default params") {//逻辑回归:默认参数
    val lr = new LogisticRegression
    //标识
    assert(lr.getLabelCol === "label")
    //特征值
    assert(lr.getFeaturesCol === "features")
    //Prediction 预测
    assert(lr.getPredictionCol === "prediction")
    //
    assert(lr.getRawPredictionCol === "rawPrediction")
    //概率
    assert(lr.getProbabilityCol === "probability")
    assert(lr.getFitIntercept)//true
    assert(lr.getStandardization)
    val model = lr.fit(dataset)
     /**
      * dd: org.apache.spark.sql.DataFrame = 
      * [label: double, features: vector, rawPrediction: vector, probability: vector, prediction: double]
      */
    val dd=model.transform(dataset).select("label", "features","rawPrediction", "probability", "prediction").collect().foreach { 
       case Row(label: Double,features:Vector,rawPrediction:Vector, probability: Vector,prediction: Double) =>
        //文档ID,text文本,probability概率,prediction 预测分类
       /**
      label=1.0, features=[1.1419053154730547],rawPrediction=[-2.7045709478814164,2.7045709478814164],probability=[0.06270417307424182,0.9372958269257582],prediction=1.0
      label=0.0, features=[0.9194079489827879],rawPrediction=[-2.3705881157542494,2.3705881157542494],probability=[0.08544317131357694,0.914556828686423],prediction=1.0
      label=0.0, features=[-0.9498666368908959],rawPrediction=[0.4353130509040084,-0.4353130509040084],probability=[0.6071416596853667,0.3928583403146333],prediction=0.0
      label=1.0, features=[-1.1069902863993377],rawPrediction=[0.6711657366448138,-0.6711657366448138],probability=[0.6617641379759286,0.3382358620240715],prediction=0.0
      label=0.0, features=[0.2809776380727795],rawPrediction=[-1.412263229860017,1.412263229860017],probability=[0.19587733100186028,0.8041226689981398],prediction=1.0
      label=1.0, features=[0.6846227956326554],rawPrediction=[-2.0181605266361613,2.0181605266361613],probability=[0.11730933034723448,0.8826906696527654],prediction=1.0
      label=1.0, features=[-0.8172214073987268],rawPrediction=[0.2362040451985834,-0.2362040451985834],probability=[0.5587779849146348,0.4412220150853652],prediction=0.0
      label=0.0, features=[-1.3966434026780434],rawPrediction=[1.1059536608131368,-1.1059536608131368],probability=[0.7513739793827359,0.24862602061726413],prediction=0.0
      label=1.0, features=[-0.19094451307087512],rawPrediction=[-0.7038777821185604,0.7038777821185604],probability=[0.3309530350546704,0.6690469649453297],prediction=1.0
      label=1.0, features=[1.4862133923906502],rawPrediction=[-3.221399476987748,3.221399476987748],probability=[0.0383683167622906,0.9616316832377094],prediction=1.0
      label=1.0, features=[0.8023071496873626],rawPrediction=[-2.194812297775065,2.194812297775065],probability=[0.10021731474468948,0.8997826852553105],prediction=1.0
      label=0.0, features=[-0.12151292466549345],rawPrediction=[-0.8080990540396891,0.8080990540396891],probability=[0.30829572362256974,0.6917042763774303],prediction=1.0
      label=1.0, features=[1.4105062239438624],rawPrediction=[-3.107758156512645,3.107758156512645],probability=[0.04278837052335011,0.9572116294766498],prediction=1.0
      label=0.0, features=[-0.6402327822135738],rawPrediction=[-0.0294672444263967,0.0294672444263967],probability=[0.4926337219086453,0.5073662780913546],prediction=1.0
      label=0.0, features=[-1.2096444592532913],rawPrediction=[0.8252562408168485,-0.8252562408168485],probability=[0.6953509536079909,0.30464904639200907],prediction=0.0
      label=1.0, features=[0.35375769787202876],rawPrediction=[-1.521510772325654,1.521510772325654],probability=[0.17923915822700226,0.8207608417729978],prediction=1.0
      label=0.0, features=[-0.4903496491990076],rawPrediction=[-0.25445145004614045,0.25445145004614045],probability=[0.43672815051472624,0.5632718494852738],prediction=1.0
      label=0.0, features=[0.5507215382743629],rawPrediction=[-1.8171661424627765,1.8171661424627765],probability=[0.13977426096634668,0.8602257390336533],prediction=1.0
      label=1.0, features=[-1.2035510019650835],rawPrediction=[0.8161095702082746,-0.8161095702082746],probability=[0.6934098863673215,0.3065901136326786],prediction=0.0
      label=1.0, features=[0.3210160806416416],rawPrediction=[-1.47236350274884,1.47236350274884],probability=[0.18658363897209224,0.8134163610279077],prediction=1.0
      label=1.0, features=[1.5511476388671834],rawPrediction=[-3.3188699499302885,3.3188699499302885],probability=[0.03492948184281332,0.9650705181571866],prediction=1.0
      label=1.0, features=[0.43853028624710505],rawPrediction=[-1.648759870141367,1.648759870141367],probability=[0.16127662711688714,0.8387233728831129],prediction=1.0
      label=1.0, features=[0.4815980608245389],rawPrediction=[-1.7134073648676247,1.7134073648676247],probability=[0.1527222870019616,0.8472777129980383],prediction=1.0
      label=1.0, features=[1.5196310789680683],rawPrediction=[-3.271561570060217,3.271561570060217],probability=[0.03655978500796004,0.96344021499204],prediction=1.0
      label=0.0, features=[-0.2768317291873249],rawPrediction=[-0.5749555565004664,0.5749555565004664],probability=[0.36009414415203267,0.6399058558479673],prediction=1.0
      label=1.0, features=[-0.08393897849486337],rawPrediction=[-0.864499959606406,0.864499959606406],probability=[0.29640003333313086,0.7035999666668692],prediction=1.0
      label=1.0, features=[1.255833005788796],rawPrediction=[-2.8755837255585965,2.8755837255585965],probability=[0.05337382933866193,0.9466261706613381],prediction=1.0
      label=0.0, features=[-0.3252727938665772],rawPrediction=[-0.5022424083493862,0.5022424083493862],probability=[0.3770138394018949,0.622986160598105],prediction=1.0
      label=0.0, features=[-0.17329033306108363],rawPrediction=[-0.7303778397667076,0.7303778397667076],probability=[0.325111818558658,0.674888181441342],prediction=1.0
      label=0.0, features=[-1.8585851445864527],rawPrediction=[1.7993578735190712,-1.7993578735190712],probability=[0.8580707514888687,0.1419292485111313],prediction=0.0
      label=1.0, features=[1.4238069456328435],rawPrediction=[-3.1277233937156463,3.1277233937156463],probability=[0.041978067441123396,0.9580219325588767],prediction=1.0
      label=0.0, features=[-1.363726024075023],rawPrediction=[1.0565425621315716,-1.0565425621315716],probability=[0.7420292702251481,0.2579707297748519],prediction=0.0
      label=0.0, features=[-1.964666098753878],rawPrediction=[1.958592196452681,-1.958592196452681],probability=[0.8763805150224312,0.12361948497756885],prediction=0.0
      label=1.0, features=[-0.9185948439341892],rawPrediction=[0.38837208192929396,-0.38837208192929396],probability=[0.5958907498149827,0.4041092501850173],prediction=0.0
      label=0.0, features=[-2.548887393384806],rawPrediction=[2.835545868523999,-2.835545868523999],probability=[0.9445667031891815,0.055433296810818544],prediction=0.0
      label=0.0, features=[-1.6309606578419305],rawPrediction=[1.4576789047131464,-1.4576789047131464],probability=[0.8111774125846752,0.18882258741532484],prediction=0.0
      label=1.0, features=[-0.12200477461989162],rawPrediction=[-0.8073607556798355,0.8073607556798355],probability=[0.30845318763632856,0.6915468123636713],prediction=1.0
      label=1.0, features=[1.289159071801577],rawPrediction=[-2.9256082902812195,2.9256082902812195],probability=[0.050902075079099066,0.949097924920901],prediction=1.0
      label=1.0, features=[-0.2691388556559934],rawPrediction=[-0.5865030535774829,0.5865030535774829],probability=[0.35743761783522787,0.6425623821647722],prediction=1.0
      label=1.0, features=[0.2574914085090889],rawPrediction=[-1.377008891395686,1.377008891395686],probability=[0.20148981453991863,0.7985101854600812],prediction=1.0
      label=0.0, features=[-0.3199143760045327],rawPrediction=[-0.5102857375874638,0.5102857375874638],probability=[0.37512654435996234,0.6248734556400376],prediction=1.0
      label=0.0, features=[-1.7684998592513064],rawPrediction=[1.664134076474569,-1.664134076474569],probability=[0.840792173364262,0.159207826635738],prediction=0.0
      label=1.0, features=[-0.4834503128592458],rawPrediction=[-0.2648077968510467,0.2648077968510467],probability=[0.43418221467392737,0.5658177853260726],prediction=1.0
      label=1.0, features=[-0.5099904653893699],rawPrediction=[-0.22496932394271418,0.22496932394271418],probability=[0.44399368223486757,0.5560063177651324],prediction=1.0
      label=1.0, features=[1.1166733769661994],rawPrediction=[-2.6666961882134332,2.6666961882134332],probability=[0.06496737577157147,0.9350326242284286],prediction=1.0
      label=1.0, features=[-0.04094720151728288],rawPrediction=[-0.9290333770560008,0.9290333770560008],probability=[0.2831208624888421,0.716879137511158],prediction=1.0
      label=0.0, features=[-1.1076715169200795],rawPrediction=[0.6721883073935726,-0.6721883073935726],probability=[0.6619929845289809,0.33800701547101897],prediction=0.0
      label=1.0, features=[1.8623214176471945],rawPrediction=[-3.7859617702067627,3.7859617702067627],probability=[0.022183749129591434,0.9778162508704085],prediction=1.0
      label=1.0, features=[1.1457411377091524],rawPrediction=[-2.710328763329557,2.710328763329557],probability=[0.06236662351134745,0.9376333764886525],prediction=1.0
      label=1.0, features=[-1.0586772048930921],rawPrediction=[0.5986446994533359,-0.5986446994533359],probability=[0.6453461736444336,0.35465382635556636],prediction=0.0
      label=1.0, features=[1.0725991339400673],rawPrediction=[-2.6005379197272642,2.6005379197272642],probability=[0.06910380876733,0.9308961912326699],prediction=1.0
      label=1.0, features=[-1.9317441520296659],rawPrediction=[1.9091742407279193,-1.9091742407279193],probability=[0.8709263496746245,0.12907365032537563],prediction=0.0
      label=1.0, features=[0.30102521611534994],rawPrediction=[-1.442355931618988,1.442355931618988],probability=[0.19118078415235168,0.8088192158476484],prediction=1.0
      label=1.0, features=[0.2475231582804265],rawPrediction=[-1.3620459078096911,1.3620459078096911],probability=[0.2039079898569551,0.7960920101430449],prediction=1.0
      label=1.0, features=[1.406156849249087],rawPrediction=[-3.1012294658523594,3.1012294658523594],probability=[0.04305656915846567,0.9569434308415344],prediction=1.0
      label=0.0, features=[-1.5202207203569256],rawPrediction=[1.2914511486985816,-1.2914511486985816],probability=[0.7843927098691734,0.21560729013082655],prediction=0.0
      label=1.0, features=[0.2709294126920897],rawPrediction=[-1.3971801984361747,1.3971801984361747],probability=[0.19826395267167526,0.8017360473283247],prediction=1.0
      label=1.0, features=[0.561249284813777],rawPrediction=[-1.832968965932619,1.832968965932619],probability=[0.13788496458744862,0.8621150354125514],prediction=1.0
      label=0.0, features=[-0.5298295780368607],rawPrediction=[-0.19518954212588657,0.19518954212588657],probability=[0.4513569539371051,0.5486430460628948],prediction=1.0
      label=0.0, features=[0.5390221914988275],rawPrediction=[-1.7996046718181247,1.7996046718181247],probability=[0.14189919475316853,0.8581008052468315],prediction=1.0
      label=1.0, features=[2.2123402141787243],rawPrediction=[-4.31136245609963,4.31136245609963],probability=[0.013237672622706951,0.986762327377293],prediction=1.0
      label=1.0, features=[-0.6329335687728442],rawPrediction=[-0.04042383243321335,0.04042383243321335],probability=[0.48989541783410395,0.510104582165896],prediction=1.0
      label=0.0, features=[-1.8831759122084633],rawPrediction=[1.836270194523731,-1.836270194523731],probability=[0.8625069924611903,0.13749300753880955],prediction=0.0
      label=1.0, features=[0.3865659853763343],rawPrediction=[-1.570758118264059,1.570758118264059],probability=[0.17210834283672774,0.8278916571632723],prediction=1.0
      label=1.0, features=[0.32582927090649455],rawPrediction=[-1.4795884103582198,1.4795884103582198],probability=[0.18548959552586086,0.814510404474139],prediction=1.0
      label=0.0, features=[-0.9013043195000002],rawPrediction=[0.36241789464794905,-0.36241789464794905],probability=[0.5896256119027153,0.41037438809728466],prediction=0.0
      label=1.0, features=[-0.002680308907617573],rawPrediction=[-0.9864744397170334,0.9864744397170334],probability=[0.2716090039785131,0.7283909960214869],prediction=1.0
      label=1.0, features=[-0.4739592549853249],rawPrediction=[-0.2790544840753363,0.2790544840753363],probability=[0.43068559711316184,0.5693144028868382],prediction=1.0
      label=1.0, features=[-0.5479781547659026],rawPrediction=[-0.16794736326033233,0.16794736326033233],probability=[0.4581115727836005,0.5418884272163995],prediction=1.0
      label=1.0, features=[-0.01910014847196348],rawPrediction=[-0.9618272063260088,0.9618272063260088],probability=[0.27651250691665435,0.7234874930833456],prediction=1.0
      label=1.0, features=[1.6468163882596327],rawPrediction=[-3.462474885000619,3.462474885000619],probability=[0.030399001430497858,0.9696009985695021],prediction=1.0
      label=0.0, features=[-1.107062592215791],rawPrediction=[0.6712742723175693,-0.6712742723175693],probability=[0.6617884313455565,0.3382115686544435],prediction=0.0
      label=1.0, features=[0.5938103926672539],rawPrediction=[-1.8818452793454252,1.8818452793454252],probability=[0.1321770646953402,0.8678229353046597],prediction=1.0
      label=0.0, features=[-0.15566462108511642],rawPrediction=[-0.7568351650683477,0.7568351650683477],probability=[0.31933378015636477,0.6806662198436352],prediction=1.0
      label=0.0, features=[0.6632872929286855],rawPrediction=[-1.9861345672716475,1.9861345672716475],probability=[0.12066640715017016,0.8793335928498298],prediction=1.0
      label=1.0, features=[1.226793360688623],rawPrediction=[-2.8319933538273974,2.8319933538273974],probability=[0.05561960210779109,0.944380397892209],prediction=1.0
      label=1.0, features=[0.8839698437730904],rawPrediction=[-2.3173932445975476,2.3173932445975476],probability=[0.08969266790000574,0.9103073320999943],prediction=1.0
      label=0.0, features=[0.22172454670212935],rawPrediction=[-1.323320535479294,1.323320535479294],probability=[0.21026637338694099,0.789733626613059],prediction=1.0
      label=1.0, features=[0.9197020859698617],rawPrediction=[-2.37102963425636,2.37102963425636],probability=[0.08540867620842725,0.9145913237915727],prediction=1.0
      label=0.0, features=[-0.7393758185888677],rawPrediction=[0.11935281846525037,-0.11935281846525037],probability=[0.5298028343284693,0.47019716567153075],prediction=0.0
      label=1.0, features=[0.803517749531419],rawPrediction=[-2.1966294858652815,2.1966294858652815],probability=[0.10005357098265742,0.8999464290173426],prediction=1.0
      label=1.0, features=[-0.2539417447630359],rawPrediction=[-0.609314892729757,0.609314892729757],probability=[0.3522154959852388,0.6477845040147612],prediction=1.0
      label=1.0, features=[-0.7638388605060555],rawPrediction=[0.15607341498658234,-0.15607341498658234],probability=[0.5389393424871357,0.4610606575128643],prediction=0.0
      label=0.0, features=[-1.8645567427274516],rawPrediction=[1.808321625723947,-1.808321625723947],probability=[0.8591589051846403,0.1408410948153596],prediction=0.0
      label=0.0, features=[-1.861306200027518],rawPrediction=[1.8034423524363612,-1.8034423524363612],probability=[0.8585674539959206,0.14143254600407945],prediction=0.0
      label=0.0, features=[-0.576599881116305],rawPrediction=[-0.12498431440422686,0.12498431440422686],probability=[0.46879453274893695,0.5312054672510631],prediction=1.0
      label=1.0, features=[-0.40899380621224757],rawPrediction=[-0.3765717936682742,0.3765717936682742],probability=[0.40695400411448074,0.5930459958855193],prediction=1.0
      label=1.0, features=[0.24846093761654187],rawPrediction=[-1.3634535748015602,1.3634535748015602],probability=[0.2036795791867364,0.7963204208132636],prediction=1.0
      label=1.0, features=[-0.48091295490277447],rawPrediction=[-0.2686165340500093,0.2686165340500093],probability=[0.43324676538259743,0.5667532346174026],prediction=1.0
      label=1.0, features=[0.44621205735391023],rawPrediction=[-1.660290701766359,1.660290701766359],probability=[0.15972297755011206,0.8402770224498879],prediction=1.0
      label=1.0, features=[-0.4465888888803913],rawPrediction=[-0.3201391608617602,0.3201391608617602],probability=[0.42064183365727476,0.5793581663427253],prediction=1.0
      label=1.0, features=[0.045638687865053575],rawPrediction=[-1.0590043561031948,1.0590043561031948],probability=[0.2574997695102409,0.742500230489759],prediction=1.0
      label=1.0, features=[0.7045663273135641],rawPrediction=[-2.048097048126177,2.048097048126177],probability=[0.11424480524360246,0.8857551947563974],prediction=1.0
      label=1.0, features=[-0.2718240183671583],rawPrediction=[-0.5824724519521987,0.5824724519521987],probability=[0.35836388115006484,0.6416361188499351],prediction=1.0
      label=1.0, features=[0.08074877915238832],rawPrediction=[-1.111706857302913,1.111706857302913],probability=[0.24755281455808453,0.7524471854419155],prediction=1.0
      label=1.0, features=[1.2590964696340183],rawPrediction=[-2.8804823943148383,2.8804823943148383],probability=[0.05312686460929857,0.9468731353907015],prediction=1.0
      label=1.0, features=[0.7635098382407334],rawPrediction=[-2.136575042385205,2.136575042385205],probability=[0.10559241495130262,0.8944075850486973],prediction=1.0
      label=1.0, features=[1.7220810801509723],rawPrediction=[-3.575452019757617,3.575452019757617],probability=[0.027239970611276313,0.9727600293887236],prediction=1.0
      label=1.0, features=[0.14595005405372477],rawPrediction=[-1.2095781570551412,1.2095781570551412],probability=[0.22977569971269843,0.7702243002873016],prediction=1.0
      label=0.0, features=[-0.9946630124621867],rawPrediction=[0.5025552867036236,-0.5025552867036236],probability=[0.6230596448869219,0.3769403551130781],prediction=0.0
 * 
 */
        println(s"label=$label,prediction=$prediction,features=$features,rawPrediction=$rawPrediction,probability=$probability")
      }
    assert(model.getThreshold === 0.5)
    assert(model.getFeaturesCol === "features")//特征
    //Prediction 预测
    assert(model.getPredictionCol === "prediction")
    assert(model.getRawPredictionCol === "rawPrediction")//原预测
    assert(model.getProbabilityCol === "probability")//可能性
    assert(model.intercept !== 0.0)//拦截
    assert(model.hasParent)
  }

  test("setThreshold, getThreshold") {//设置,获得门槛
    val lr = new LogisticRegression
    // default
    assert(lr.getThreshold === 0.5, "LogisticRegression.threshold should default to 0.5")
    withClue("LogisticRegression should not have thresholds set by default.") {
      intercept[java.util.NoSuchElementException] { // Note: The exception type may change in future
        lr.getThresholds
      }
    }
    // Set via threshold.
    // Intuition: Large threshold or large thresholds(1) makes class 0 more likely.
    lr.setThreshold(1.0)
    assert(lr.getThresholds === Array(0.0, 1.0))
    lr.setThreshold(0.0)
    assert(lr.getThresholds === Array(1.0, 0.0))
    lr.setThreshold(0.5)
    assert(lr.getThresholds === Array(0.5, 0.5))
    // Set via thresholds
    //通过设置阈值
    val lr2 = new LogisticRegression
    lr2.setThresholds(Array(0.3, 0.7))
    val expectedThreshold = 1.0 / (1.0 + 0.3 / 0.7)
    assert(lr2.getThreshold ~== expectedThreshold relTol 1E-7)
    // thresholds and threshold must be consistent
    //阈值和阈值必须是一致的
    lr2.setThresholds(Array(0.1, 0.2, 0.3))
    withClue("getThreshold should throw error if thresholds has length != 2.") {
      intercept[IllegalArgumentException] {
        lr2.getThreshold
      }
    }
    // thresholds and threshold must be consistent: values
    //阈值和阈值必须是一致的：值
    withClue("fit with ParamMap should throw error if threshold, thresholds do not match.") {
      intercept[IllegalArgumentException] {
        val lr2model = lr2.fit(dataset,
          lr2.thresholds -> Array(0.3, 0.7), lr2.threshold -> (expectedThreshold / 2.0))
        lr2model.getThreshold
      }
    }
  }
  //逻辑回归模型不适合拦截时,fitintercept关闭
  test("logistic regression doesn't fit intercept when fitIntercept is off") {
    val lr = new LogisticRegression
    lr.setFitIntercept(false)
    val model = lr.fit(dataset)
    assert(model.intercept === 0.0)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }

  test("logistic regression with setters") {//逻辑回归设置
    // Set params, train, and check as many params as we can.
    //设置参数,训练并检查许多参数
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setProbabilityCol("myProbability")
    val model = lr.fit(dataset)
    val parent = model.parent.asInstanceOf[LogisticRegression]
    assert(parent.getMaxIter === 10)
    assert(parent.getRegParam === 1.0)
    assert(parent.getThreshold === 0.6)
    assert(model.getThreshold === 0.6)

    // Modify model params, and check that the params worked.
    //修改模型参数,并检查工作的参数
    model.setThreshold(1.0)
    val predAllZero = model.transform(dataset)
      .select("prediction", "myProbability")
      .collect()
      .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predAllZero.forall(_ === 0),
      s"With threshold=1.0, expected predictions to be all 0, but only" +
      s" ${predAllZero.count(_ === 0)} of ${dataset.count()} were 0.")
    // Call transform with params, and check that the params worked.
      //调用变换参数,并检查工作的参数
    val predNotAllZero =
      model.transform(dataset, model.threshold -> 0.0,
        model.probabilityCol -> "myProb")
        .select("prediction", "myProb")
        .collect()
        .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predNotAllZero.exists(_ !== 0.0))

    // Call fit() with new params, and check as many params as we can.
    lr.setThresholds(Array(0.6, 0.4))
    val model2 = lr.fit(dataset, lr.maxIter -> 5, lr.regParam -> 0.1,
      lr.probabilityCol -> "theProb")
    val parent2 = model2.parent.asInstanceOf[LogisticRegression]
    assert(parent2.getMaxIter === 5)
    assert(parent2.getRegParam === 0.1)
    assert(parent2.getThreshold === 0.4)
    assert(model2.getThreshold === 0.4)
    assert(model2.getProbabilityCol === "theProb")
  }

  test("logistic regression: Predictor, Classifier methods") {//逻辑回归:预测,分类方法
    val sqlContext = this.sqlContext
    val lr = new LogisticRegression

    val model = lr.fit(dataset)
    assert(model.numClasses === 2)

    val threshold = model.getThreshold
    val results = model.transform(dataset)

    // Compare rawPrediction with probability
    //用概率比较原始预测
    results.select("rawPrediction", "probability").collect().foreach {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
    }

    // Compare prediction with probability
    //用概率比较预测
    results.select("prediction", "probability").collect().foreach {
      case Row(pred: Double, prob: Vector) =>
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }
  }

  test("MultiClassSummarizer") {//多类总结
    val summarizer1 = (new MultiClassSummarizer)
      .add(0.0).add(3.0).add(4.0).add(3.0).add(6.0)
    assert(summarizer1.histogram.zip(Array[Long](1, 0, 0, 2, 1, 0, 1)).forall(x => x._1 === x._2))
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0).add(5.0).add(3.0).add(0.0).add(4.0).add(1.0)
    assert(summarizer2.histogram.zip(Array[Long](1, 2, 0, 1, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizer2.countInvalid === 0)
    assert(summarizer2.numClasses === 6)

    val summarizer3 = (new MultiClassSummarizer)
      .add(0.0).add(1.3).add(5.2).add(2.5).add(2.0).add(4.0).add(4.0).add(4.0).add(1.0)
    assert(summarizer3.histogram.zip(Array[Long](1, 1, 1, 0, 3)).forall(x => x._1 === x._2))
    assert(summarizer3.countInvalid === 3)
    assert(summarizer3.numClasses === 5)

    val summarizer4 = (new MultiClassSummarizer)
      .add(3.1).add(4.3).add(2.0).add(1.0).add(3.0)
    assert(summarizer4.histogram.zip(Array[Long](0, 1, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizer4.countInvalid === 2)
    assert(summarizer4.numClasses === 4)

    // small map merges large one
    //小Map合并大Map
    val summarizerA = summarizer1.merge(summarizer2)
    assert(summarizerA.hashCode() === summarizer2.hashCode())
    assert(summarizerA.histogram.zip(Array[Long](2, 2, 0, 3, 2, 1, 1)).forall(x => x._1 === x._2))
    assert(summarizerA.countInvalid === 0)
    assert(summarizerA.numClasses === 7)

    // large map merges small one
    val summarizerB = summarizer3.merge(summarizer4)
    assert(summarizerB.hashCode() === summarizer3.hashCode())
    assert(summarizerB.histogram.zip(Array[Long](1, 2, 2, 1, 3)).forall(x => x._1 === x._2))
    assert(summarizerB.countInvalid === 5)
    assert(summarizerB.numClasses === 5)
  }
  //不规则截取的二元逻辑回归
  test("binary logistic regression with intercept without regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 0))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)  2.8366423
       data.V2     -0.5895848
       data.V3      0.8931147
       data.V4     -0.3925051
       data.V5     -0.7996864
     */
    val interceptR = 2.8366423
    val weightsR = Vectors.dense(-0.5895848, 0.8931147, -0.3925051, -0.7996864)

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.weights ~= weightsR relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.weights ~= weightsR relTol 1E-3)
  }
  //不规则正则化的二元逻辑回归
  test("binary logistic regression without intercept without regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights =
           coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 0, intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)   .
       data.V2     -0.3534996
       data.V3      1.2964482
       data.V4     -0.3571741
       data.V5     -0.7407946
     */
    val interceptR = 0.0
    val weightsR = Vectors.dense(-0.3534996, 1.2964482, -0.3571741, -0.7407946)

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.weights ~= weightsR relTol 1E-2)

    // Without regularization, with or without standardization should converge to the same solution.
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.weights ~= weightsR relTol 1E-2)
  }

  test("binary logistic regression with intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept) -0.05627428
       data.V2       .
       data.V3       .
       data.V4     -0.04325749
       data.V5     -0.02481551
     */
    val interceptR1 = -0.05627428
    val weightsR1 = Vectors.dense(0.0, 0.0, -0.04325749, -0.02481551)

    assert(model1.intercept ~== interceptR1 relTol 1E-2)
    assert(model1.weights ~= weightsR1 absTol 2E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept)  0.3722152
       data.V2       .
       data.V3       .
       data.V4     -0.1665453
       data.V5       .
     */
    val interceptR2 = 0.3722152
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.1665453, 0.0)

    assert(model2.intercept ~== interceptR2 relTol 1E-2)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }

  test("binary logistic regression without intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2       .
       data.V3       .
       data.V4     -0.05189203
       data.V5     -0.03891782
     */
    val interceptR1 = 0.0
    val weightsR1 = Vectors.dense(0.0, 0.0, -0.05189203, -0.03891782)

    assert(model1.intercept ~== interceptR1 relTol 1E-3)
    assert(model1.weights ~= weightsR1 absTol 1E-3)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1, lambda = 0.12,
           intercept=FALSE, standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2       .
       data.V3       .
       data.V4     -0.08420782
       data.V5       .
     */
    val interceptR2 = 0.0
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.08420782, 0.0)

    assert(model2.intercept ~== interceptR2 absTol 1E-3)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }

  test("binary logistic regression with intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.15021751
       data.V2     -0.07251837
       data.V3      0.10724191
       data.V4     -0.04865309
       data.V5     -0.10062872
     */
    val interceptR1 = 0.15021751
    val weightsR1 = Vectors.dense(-0.07251837, 0.10724191, -0.04865309, -0.10062872)

    assert(model1.intercept ~== interceptR1 relTol 1E-3)
    assert(model1.weights ~= weightsR1 relTol 1E-3)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.48657516
       data.V2     -0.05155371
       data.V3      0.02301057
       data.V4     -0.11482896
       data.V5     -0.06266838
     */
    val interceptR2 = 0.48657516
    val weightsR2 = Vectors.dense(-0.05155371, 0.02301057, -0.11482896, -0.06266838)

    assert(model2.intercept ~== interceptR2 relTol 1E-3)
    assert(model2.weights ~= weightsR2 relTol 1E-3)
  }

  test("binary logistic regression without intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37,
           intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2     -0.06099165
       data.V3      0.12857058
       data.V4     -0.04708770
       data.V5     -0.09799775
     */
    val interceptR1 = 0.0
    val weightsR1 = Vectors.dense(-0.06099165, 0.12857058, -0.04708770, -0.09799775)

    assert(model1.intercept ~== interceptR1 absTol 1E-3)
    assert(model1.weights ~= weightsR1 relTol 1E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0, lambda = 1.37,
           intercept=FALSE, standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                             s0
       (Intercept)   .
       data.V2     -0.005679651
       data.V3      0.048967094
       data.V4     -0.093714016
       data.V5     -0.053314311
     */
    val interceptR2 = 0.0
    val weightsR2 = Vectors.dense(-0.005679651, 0.048967094, -0.093714016, -0.053314311)

    assert(model2.intercept ~== interceptR2 absTol 1E-3)
    assert(model2.weights ~= weightsR2 relTol 1E-2)
  }

  test("binary logistic regression with intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.57734851
       data.V2     -0.05310287
       data.V3       .
       data.V4     -0.08849250
       data.V5     -0.15458796
     */
    val interceptR1 = 0.57734851
    val weightsR1 = Vectors.dense(-0.05310287, 0.0, -0.08849250, -0.15458796)

    assert(model1.intercept ~== interceptR1 relTol 6E-3)
    assert(model1.weights ~== weightsR1 absTol 5E-3)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21,
           standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)  0.51555993
       data.V2       .
       data.V3       .
       data.V4     -0.18807395
       data.V5     -0.05350074
     */
    val interceptR2 = 0.51555993
    val weightsR2 = Vectors.dense(0.0, 0.0, -0.18807395, -0.05350074)

    assert(model2.intercept ~== interceptR2 relTol 6E-3)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }

  test("binary logistic regression without intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21,
           intercept=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2     -0.001005743
       data.V3      0.072577857
       data.V4     -0.081203769
       data.V5     -0.142534158
     */
    val interceptR1 = 0.0
    val weightsR1 = Vectors.dense(-0.001005743, 0.072577857, -0.081203769, -0.142534158)

    assert(model1.intercept ~== interceptR1 relTol 1E-3)
    assert(model1.weights ~= weightsR1 absTol 1E-2)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 0.38, lambda = 0.21,
           intercept=FALSE, standardize=FALSE))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept)   .
       data.V2       .
       data.V3      0.03345223
       data.V4     -0.11304532
       data.V5       .
     */
    val interceptR2 = 0.0
    val weightsR2 = Vectors.dense(0.0, 0.03345223, -0.11304532, 0.0)

    assert(model2.intercept ~== interceptR2 absTol 1E-3)
    assert(model2.weights ~= weightsR2 absTol 1E-3)
  }

  test("binary logistic regression with intercept with strong L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    val histogram = binaryDataset.map { case Row(label: Double, features: Vector) => label }
      .treeAggregate(new MultiClassSummarizer)(
        seqOp = (c, v) => (c, v) match {
          case (classSummarizer: MultiClassSummarizer, label: Double) => classSummarizer.add(label)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (classSummarizer1: MultiClassSummarizer, classSummarizer2: MultiClassSummarizer) =>
            classSummarizer1.merge(classSummarizer2)
        }).histogram

    /*
       For binary logistic regression with strong L1 regularization, all the weights will be zeros.
       As a result,
       {{{
       P(0) = 1 / (1 + \exp(b)), and
       P(1) = \exp(b) / (1 + \exp(b))
       }}}, hence
       {{{
       b = \log{P(1) / P(0)} = \log{count_1 / count_0}
       }}}
     */
    val interceptTheory = math.log(histogram(1).toDouble / histogram(0).toDouble)
    val weightsTheory = Vectors.dense(0.0, 0.0, 0.0, 0.0)

    assert(model1.intercept ~== interceptTheory relTol 1E-5)
    assert(model1.weights ~= weightsTheory absTol 1E-6)

    assert(model2.intercept ~== interceptTheory relTol 1E-5)
    assert(model2.weights ~= weightsTheory absTol 1E-6)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       weights = coef(glmnet(features,label, family="binomial", alpha = 1.0, lambda = 6.0))
       weights

       5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
       (Intercept) -0.2480643
       data.V2      0.0000000
       data.V3       .
       data.V4       .
       data.V5       .
     */
    val interceptR = -0.248065
    val weightsR = Vectors.dense(0.0, 0.0, 0.0, 0.0)

    assert(model1.intercept ~== interceptR relTol 1E-5)
    assert(model1.weights ~== weightsR absTol 1E-6)
  }

  test("evaluate on test set") {
    // Evaluate on test set should be same as that of the transformed training data.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
    val model = lr.fit(dataset)
    val summary = model.summary.asInstanceOf[BinaryLogisticRegressionSummary]

    val sameSummary = model.evaluate(dataset).asInstanceOf[BinaryLogisticRegressionSummary]
    assert(summary.areaUnderROC === sameSummary.areaUnderROC)
    assert(summary.roc.collect() === sameSummary.roc.collect())
    assert(summary.pr.collect === sameSummary.pr.collect())
    assert(
      summary.fMeasureByThreshold.collect() === sameSummary.fMeasureByThreshold.collect())
    assert(summary.recallByThreshold.collect() === sameSummary.recallByThreshold.collect())
    assert(
      summary.precisionByThreshold.collect() === sameSummary.precisionByThreshold.collect())
  }

  test("statistics on training data") {
    // Test that loss is monotonically decreasing.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
    val model = lr.fit(dataset)
    assert(
      model.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))

  }
}
