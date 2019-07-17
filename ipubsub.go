// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package libqueue

type IPubSub interface {
	SetClient(iClient *IClient) IPubSub
	SetChannel(channel string) IPubSub
	Publish(msg string) error
	ReceiveMessage() chan string
	Close() error
}
