package core

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/xdg-go/scram"
)

var (
	scramSHA256 scram.HashGeneratorFcn = sha256.New
	scramSHA512 scram.HashGeneratorFcn = sha512.New
)

// xdgSCRAMClient 实现 sarama.SCRAMClient，用于 SASL SCRAM-SHA-256/512。
type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	hashGen scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.hashGen.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (string, error) {
	return x.ClientConversation.Step(challenge)
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
