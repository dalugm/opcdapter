package client

import (
	"errors"
	"time"
)

var (
	ErrConnectionNotFound  = errors.New("connection is not registered")
	ErrWriteOutcomeUnknown = errors.New("write outcome is unknown")
)

// ConnectionConfig identifies one registered OPC DA server. SetConnections
// replaces the complete registry; separate owners must use separate servers.
type ConnectionConfig struct {
	UID              string
	Host             string
	Domain           string
	Username         string
	Password         string
	CLSID            string
	ProgID           string
	SamplingInterval time.Duration
}

type Quality struct {
	Good        bool
	Raw         uint32
	Description string
}

type Sample struct {
	Address    string
	Value      any
	SourceTime time.Time
	Quality    Quality
	Err        error
}

// WriteTarget supports Boolean and Float64 values. Results retain input order.
type WriteTarget struct {
	ConnectionUID string
	Address       string
	Value         any
}

type WriteState uint8

const (
	WriteNotAttempted WriteState = iota
	WriteConfirmed
	WriteRejected
	WriteUnknown
)

type WriteResult struct {
	State WriteState
	Err   error
}

type ItemError struct {
	ItemID string
	Error  string
}
