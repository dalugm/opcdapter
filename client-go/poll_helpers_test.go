package client

import rpc "github.com/dalugm/opcdapter/gen/go/opcda/v1"

func pollAck(request *rpc.PollRequest) *rpc.PollResponse {
	validations := make([]*rpc.ItemValidationResult, len(request.ItemIds))
	for i, id := range request.ItemIds {
		validations[i] = &rpc.ItemValidationResult{ItemId: id}
	}
	return &rpc.PollResponse{
		Payload: &rpc.PollResponse_Ack{
			Ack: &rpc.PollAck{RequestId: request.RequestId, Validations: validations},
		},
	}
}

func pollData(id string, sequence uint64, values ...*rpc.DataValue) *rpc.PollResponse {
	return &rpc.PollResponse{
		Payload: &rpc.PollResponse_Data{
			Data: &rpc.PollData{
				Sequence: sequence,
				Batch:    &rpc.ConnectionDataBatch{ConnectionId: id, Values: values},
			},
		},
	}
}

func goodPollValue(id string) *rpc.DataValue {
	return &rpc.DataValue{
		ItemId:  id,
		Quality: 0xC0,
		Value:   &rpc.DataValue_DoubleValue{DoubleValue: 12},
	}
}
