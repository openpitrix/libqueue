// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package test

import (
	"testing"
	"time"

	"openpitrix.io/logger"

	q "openpitrix.io/libqueue/queue"
)

func TestPublish4Redis(t *testing.T) {
	pubsubConnStr := "redis://192.168.0.6:6379"
	pubsubType := "redis"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	ipubsub, _ := q.NewIPubSub(pubsubType, &iClient)

	ipubsub.SetChannel("channel1")
	ipubsub.Publish("data1")

	defer ipubsub.Close()
}

func TestReceiveMessage4Redis(t *testing.T) {
	pubsubConnStr := "redis://192.168.0.6:6379"
	pubsubType := "redis"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	ipubsub, _ := q.NewIPubSub(pubsubType, &iClient)
	ipubsub.SetChannel("channel1")

	msgChan := ipubsub.ReceiveMessage()
	time.Sleep(1 * time.Second)

	ipubsub.Publish("data1")
	outmsg := <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Publish("data2")
	outmsg = <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Publish("data3")
	outmsg = <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Publish("data4")
	outmsg = <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

}

/*****************************************************************************************************************/

func TestPublish4Etcd(t *testing.T) {
	pubsubConnStr := "192.168.0.6:12379"
	pubsubType := "etcd"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	ipubsub, _ := q.NewIPubSub(pubsubType, &iClient)
	ipubsub.SetChannel("channel1")

	ipubsub.Publish("data1")

	ipubsub.Close()
}

func TestReceiveMessage4Etcd(t *testing.T) {
	pubsubConnStr := "192.168.0.6:12379"
	pubsubType := "etcd"
	pubsubConfigMap := map[string]interface{}{
		"connStr": pubsubConnStr}
	iClient, _ := q.NewIClient(pubsubType, pubsubConfigMap)
	ipubsub, _ := q.NewIPubSub(pubsubType, &iClient)
	ipubsub.SetChannel("channel1")

	msgChan := ipubsub.ReceiveMessage()
	time.Sleep(1 * time.Second)

	ipubsub.Publish("data1")
	outmsg := <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Publish("data2")
	outmsg = <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Publish("data3")
	outmsg = <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Publish("data4")
	outmsg = <-msgChan
	logger.Infof(nil, "outmsg=[%+v]. ", outmsg)

	ipubsub.Close()

}
