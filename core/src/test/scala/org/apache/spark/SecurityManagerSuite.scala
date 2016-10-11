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

package org.apache.spark

import java.io.File

import org.apache.spark.util.Utils

class SecurityManagerSuite extends SparkFunSuite {

  test("set security with conf") {//设置安全配置
    val conf = new SparkConf
     //是否启用内部身份验证
    conf.set("spark.authenticate", "true")
     //设置组件之间进行身份验证的密钥
    conf.set("spark.authenticate.secret", "good")
     //Spark webUI存取权限是否启用。如果启用，在用户浏览web界面的时候会检查用户是否有访问权限
    conf.set("spark.ui.acls.enable", "true")
    //以逗号分隔Spark webUI访问用户的列表。默认情况下只有启动Spark job的用户才有访问权限
    conf.set("spark.ui.view.acls", "user1,user2")
    val securityManager = new SecurityManager(conf);
    assert(securityManager.isAuthenticationEnabled() === true)
    assert(securityManager.aclsEnabled() === true)
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)
    assert(securityManager.checkUIViewPermissions("user3") === false)
  }

  test("set security with api") {//设置安全性API
    val conf = new SparkConf
    //以逗号分隔Spark webUI访问用户的列表。默认情况下只有启动Spark job的用户才有访问权限
    conf.set("spark.ui.view.acls", "user1,user2")
    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setAcls(false)
    assert(securityManager.aclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    assert(securityManager.checkUIViewPermissions("user4") === true)

    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setViewAcls(Set[String]("user5"), "user6,user7")
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user5") === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkUIViewPermissions("user7") === true)
    assert(securityManager.checkUIViewPermissions("user8") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)
  }

  test("set security modify acls") {//设置安全修改ACL
    val conf = new SparkConf
    conf.set("spark.modify.acls", "user1,user2")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setAcls(false)
    assert(securityManager.aclsEnabled() === false)

    // acls are off so doesn't matter what view acls set to
    //ACL是不管怎样查看ACL设置
    assert(securityManager.checkModifyPermissions("user4") === true)

    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)
    securityManager.setModifyAcls(Set("user5"), "user6,user7")
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user5") === true)
    assert(securityManager.checkModifyPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user7") === true)
    assert(securityManager.checkModifyPermissions("user8") === false)
    assert(securityManager.checkModifyPermissions(null) === true)
  }

  test("set security admin acls") {//设置安全管理系统
    val conf = new SparkConf
    //以逗号分隔Spark webUI访问用户的列表。默认情况下只有启动Spark job的用户才有访问权限
    conf.set("spark.admin.acls", "user1,user2")
    conf.set("spark.ui.view.acls", "user3")
    conf.set("spark.modify.acls", "user4")

    val securityManager = new SecurityManager(conf);
    securityManager.setAcls(true)
    assert(securityManager.aclsEnabled() === true)

    assert(securityManager.checkModifyPermissions("user1") === true)
    assert(securityManager.checkModifyPermissions("user2") === true)
    assert(securityManager.checkModifyPermissions("user4") === true)
    assert(securityManager.checkModifyPermissions("user3") === false)
    assert(securityManager.checkModifyPermissions("user5") === false)
    assert(securityManager.checkModifyPermissions(null) === true)
    assert(securityManager.checkUIViewPermissions("user1") === true)
    assert(securityManager.checkUIViewPermissions("user2") === true)
    assert(securityManager.checkUIViewPermissions("user3") === true)
    assert(securityManager.checkUIViewPermissions("user4") === false)
    assert(securityManager.checkUIViewPermissions("user5") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)

    securityManager.setAdminAcls("user6")
    securityManager.setViewAcls(Set[String]("user8"), "user9")
    securityManager.setModifyAcls(Set("user11"), "user9")
    assert(securityManager.checkModifyPermissions("user6") === true)
    assert(securityManager.checkModifyPermissions("user11") === true)
    assert(securityManager.checkModifyPermissions("user9") === true)
    assert(securityManager.checkModifyPermissions("user1") === false)
    assert(securityManager.checkModifyPermissions("user4") === false)
    assert(securityManager.checkModifyPermissions(null) === true)
    assert(securityManager.checkUIViewPermissions("user6") === true)
    assert(securityManager.checkUIViewPermissions("user8") === true)
    assert(securityManager.checkUIViewPermissions("user9") === true)
    assert(securityManager.checkUIViewPermissions("user1") === false)
    assert(securityManager.checkUIViewPermissions("user3") === false)
    assert(securityManager.checkUIViewPermissions(null) === true)

  }

  test("ssl on setup") {
    val conf = SSLSampleConfigs.sparkSSLConfig()
    val expectedAlgorithms = Set(
    "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
    "TLS_RSA_WITH_AES_256_CBC_SHA256",
    "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
    "SSL_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
    "SSL_RSA_WITH_AES_256_CBC_SHA256",
    "SSL_DHE_RSA_WITH_AES_256_CBC_SHA256",
    "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
    "SSL_DHE_RSA_WITH_AES_128_CBC_SHA256")

    val securityManager = new SecurityManager(conf)

    assert(securityManager.fileServerSSLOptions.enabled === true)
    assert(securityManager.akkaSSLOptions.enabled === true)

    assert(securityManager.sslSocketFactory.isDefined === true)
    assert(securityManager.hostnameVerifier.isDefined === true)

    assert(securityManager.fileServerSSLOptions.trustStore.isDefined === true)
    assert(securityManager.fileServerSSLOptions.trustStore.get.getName === "truststore")
    assert(securityManager.fileServerSSLOptions.keyStore.isDefined === true)
    assert(securityManager.fileServerSSLOptions.keyStore.get.getName === "keystore")
    assert(securityManager.fileServerSSLOptions.trustStorePassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.keyStorePassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.keyPassword === Some("password"))
    assert(securityManager.fileServerSSLOptions.protocol === Some("TLSv1.2"))
    assert(securityManager.fileServerSSLOptions.enabledAlgorithms === expectedAlgorithms)

    assert(securityManager.akkaSSLOptions.trustStore.isDefined === true)
    assert(securityManager.akkaSSLOptions.trustStore.get.getName === "truststore")
    assert(securityManager.akkaSSLOptions.keyStore.isDefined === true)
    assert(securityManager.akkaSSLOptions.keyStore.get.getName === "keystore")
    assert(securityManager.akkaSSLOptions.trustStorePassword === Some("password"))
    assert(securityManager.akkaSSLOptions.keyStorePassword === Some("password"))
    assert(securityManager.akkaSSLOptions.keyPassword === Some("password"))
    assert(securityManager.akkaSSLOptions.protocol === Some("TLSv1.2"))
    assert(securityManager.akkaSSLOptions.enabledAlgorithms === expectedAlgorithms)
  }

  test("ssl off setup") {
    val file = File.createTempFile("SSLOptionsSuite", "conf", Utils.createTempDir())

    System.setProperty("spark.ssl.configFile", file.getAbsolutePath)
    val conf = new SparkConf()

    val securityManager = new SecurityManager(conf)

    assert(securityManager.fileServerSSLOptions.enabled === false)
    assert(securityManager.akkaSSLOptions.enabled === false)
    assert(securityManager.sslSocketFactory.isDefined === false)
    assert(securityManager.hostnameVerifier.isDefined === false)
  }

}

