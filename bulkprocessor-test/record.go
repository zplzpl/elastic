package main

import (
	"math/rand"
	"time"
)

type Record struct {
	ID      int64     `json:"id"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

func MakeRecord() *Record {
	return &Record{
		ID:      rand.Int63(),
		Message: randomString(40),
		Time:    time.Now().UTC(),
	}
}

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

var alphabet = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
