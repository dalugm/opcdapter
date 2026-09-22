package client

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	rpc "github.com/dalugm/opcdapter/gen/go/opcda/v1"
)

// PollStream validates the acknowledgement and ordered snapshots. Recv must
// have one caller. Cancel the context passed to Poll to release the stream.
type PollStream struct {
	stream       grpc.ServerStreamingClient[rpc.PollResponse]
	request      *rpc.PollRequest
	accepted     []string
	acknowledged bool
	sequence     uint64
}

type PollEvent struct {
	Acknowledgement bool
	Accepted        []string
	Failures        []ItemError
	Samples         []Sample
	BatchError      error
}

func (c *Client) Poll(ctx context.Context, connectionID string, itemIDs []string) (*PollStream, error) {
	request := &rpc.PollRequest{RequestId: newRequestID("poll"), ConnectionId: connectionID, ItemIds: append([]string(nil), itemIDs...)}
	stream, err := c.openPoll(ctx, request)
	if err != nil {
		return nil, err
	}
	return &PollStream{stream: stream, request: request}, nil
}

func (s *PollStream) Recv() (PollEvent, error) {
	response, err := s.stream.Recv()
	if err != nil {
		return PollEvent{}, err
	}
	if !s.acknowledged {
		accepted, failures, err := validatePollAck(s.request, response.GetAck())
		if err != nil {
			return PollEvent{}, err
		}
		s.accepted, s.acknowledged = accepted, true
		return PollEvent{Acknowledgement: true, Accepted: append([]string(nil), accepted...), Failures: failures}, nil
	}
	data := response.GetData()
	if data == nil || data.GetBatch() == nil || data.GetSequence() <= s.sequence {
		return PollEvent{}, errors.New("invalid opcdapter poll data or sequence")
	}
	if data.Batch.ConnectionId != s.request.ConnectionId {
		return PollEvent{}, errors.New("opcdapter poll connection ID mismatch")
	}
	s.sequence = data.Sequence
	samples, batchErr := readBatchSamples(s.request.ConnectionId, s.accepted, data.Batch)
	return PollEvent{Samples: samples, BatchError: batchErr}, nil
}

// The registry gate protects stream creation only. Holding it during Recv
// would prevent registry reconciliation and independent unary reads/writes.
func (c *Client) openPoll(
	ctx context.Context,
	request *rpc.PollRequest,
) (grpc.ServerStreamingClient[rpc.PollResponse], error) {
	c.gate.RLock()
	defer c.gate.RUnlock()
	if err := c.dataPlaneReady(request.ConnectionId); err != nil {
		return nil, err
	}
	if err := validateItemIDs(request.ItemIds); err != nil {
		return nil, err
	}
	return c.rpc.Poll(ctx, request)
}

func validatePollAck(
	request *rpc.PollRequest,
	ack *rpc.PollAck,
) ([]string, []ItemError, error) {
	if ack == nil || ack.RequestId != request.RequestId {
		return nil, nil, errors.New("opcdapter poll acknowledgement mismatch")
	}
	requested := make(map[string]bool, len(request.ItemIds))
	for _, id := range request.ItemIds {
		requested[id] = true
	}
	var accepted []string
	var failures []ItemError
	for _, validation := range ack.Validations {
		if validation == nil || !requested[validation.ItemId] {
			return nil, nil, errors.New(
				"opcdapter poll validation contains duplicate or unrequested item",
			)
		}
		delete(requested, validation.ItemId)
		if validation.Error != nil {
			failures = append(
				failures,
				ItemError{
					ItemID: validation.ItemId,
					Error:  remoteError(validation.Error).Error(),
				},
			)
		} else {
			accepted = append(accepted, validation.ItemId)
		}
	}
	if len(requested) > 0 {
		return nil, nil, errors.New("opcdapter poll validation is incomplete")
	}
	return accepted, failures, nil
}
