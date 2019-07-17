// Copyright 2019 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package util

import (
	"errors"
	"net"

	"github.com/sony/sonyflake"
)

var sf *sonyflake.Sonyflake
var upperMachineID uint16

func init() {
	var st sonyflake.Settings
	sf = sonyflake.NewSonyflake(st)

	if sf == nil {
		sf = sonyflake.NewSonyflake(sonyflake.Settings{
			MachineID: lower16BitIP,
		})
		upperMachineID, _ = upper16BitIP()
	}
}

func lower16BitIP() (uint16, error) {
	ip, err := IPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[2])<<8 + uint16(ip[3]), nil
}

func upper16BitIP() (uint16, error) {
	ip, err := IPv4()
	if err != nil {
		return 0, err
	}

	return uint16(ip[0])<<8 + uint16(ip[1]), nil
}

func IPv4() (net.IP, error) {
	as, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range as {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}

		ip := ipnet.IP.To4()
		return ip, nil

	}
	return nil, errors.New("no ip address")
}

func GetIntId() uint64 {
	id, err := sf.NextID()
	if err != nil {
		panic(err)
	}
	return id
}
