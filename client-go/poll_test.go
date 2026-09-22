package client

import (
	rpc "github.com/dalugm/opcdapter/gen/go/opcda/v1"
	"slices"
	"testing"
)

func TestPollAcknowledgementRequiresExactItemCoverage(t *testing.T) {
	request := &rpc.PollRequest{RequestId: "request", ItemIds: []string{"A", "B"}}
	good := &rpc.ItemValidationResult{ItemId: "A"}
	bad := &rpc.ItemValidationResult{
		ItemId: "B",
		Error:  &rpc.ErrorInfo{Code: rpc.ErrorCode_ERROR_CODE_OPC_FAILURE, Message: "unknown item"},
	}
	accepted, failures, err := validatePollAck(
		request,
		&rpc.PollAck{RequestId: "request", Validations: []*rpc.ItemValidationResult{good, bad}},
	)
	if err != nil || !slices.Equal(accepted, []string{"A"}) || len(failures) != 1 ||
		failures[0].ItemID != "B" {
		t.Fatalf("accepted=%v failures=%v err=%v", accepted, failures, err)
	}
	for _, ack := range []*rpc.PollAck{
		nil,
		{RequestId: "wrong"},
		{RequestId: "request", Validations: []*rpc.ItemValidationResult{good}},
		{RequestId: "request", Validations: []*rpc.ItemValidationResult{good, good}},
		{RequestId: "request", Validations: []*rpc.ItemValidationResult{good, {ItemId: "other"}}},
		{RequestId: "request", Validations: []*rpc.ItemValidationResult{good, nil}},
	} {
		if _, _, err := validatePollAck(request, ack); err == nil {
			t.Errorf("accepted malformed ack: %v", ack)
		}
	}
}
