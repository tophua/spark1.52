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

package org.apache.spark.streaming.mqtt

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Input stream that subscribe messages from a Mqtt Broker.
 * Uses eclipse paho as MqttClient http://www.eclipse.org/paho/
 * @param brokerUrl Url of remote mqtt publisher
 * @param topic topic name to subscribe to
 * @param storageLevel RDD storage level.
 */

private[streaming]
class MQTTInputDStream(
    @transient ssc_ : StreamingContext,
    brokerUrl: String,//MQTT服务器提供的消息消费地址
    topic: String,//订阅主题名称
    storageLevel: StorageLevel//RDD存储级别
  ) extends ReceiverInputDStream[String](ssc_) {

  private[streaming] override def name: String = s"MQTT stream [$id]"

  def getReceiver(): Receiver[String] = {
    
    new MQTTReceiver(brokerUrl, topic, storageLevel)
  }
}

private[streaming]
class MQTTReceiver(
    brokerUrl: String,
    topic: String,
    storageLevel: StorageLevel
  ) extends Receiver[String](storageLevel) {

  def onStop() {

  }

  def onStart() {

    // Set up persistence for messages
    //负责对消息的持久化
    val persistence = new MemoryPersistence()

    // Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    //初始化MQTT客户端指定brokerurl,ClientID和MqttClient持久化
    val client = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)

    // Callback automatically triggers as and when new message arrives on specified topic
    //用于消息到达时自动将消息存储到Spark内存
    val callback = new MqttCallback() {

      // Handles Mqtt message
      //处理Mqtt消息
      override def messageArrived(topic: String, message: MqttMessage) {
        //将消息存储到Spark内存,getPayload获取文件字节数组
        store(new String(message.getPayload(), "utf-8"))
      }

      override def deliveryComplete(token: IMqttDeliveryToken) {
      }

      override def connectionLost(cause: Throwable) {
        restart("Connection lost ", cause)//连接丢失
      }
    }

    // Set up callback for MqttClient. This needs to happen before
    // connecting or subscribing, otherwise messages may be lost
    //设置回调mqttclient,这需要连接或订阅消息之前发生,否则可能会丢失
    client.setCallback(callback)

    // Connect to MqttBroker
    //用于连接MQTT服务器
    client.connect()

    // Subscribe to Mqtt topic
    //用于订阅消息主题
    client.subscribe(topic)

  }
}
