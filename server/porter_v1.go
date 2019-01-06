package server

import (
	"context"

	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	pb "github.com/joshchu00/finance-protobuf/porter"
)

type PorterV1Server struct {
	CassandraClient *cassandra.Client
}

func (s *PorterV1Server) GetSymbol(ctx context.Context, in *pb.GetSymbolRequest) (out *pb.GetSymbolReply, err error) {

	var rs []*cassandra.Record
	rs, err = s.CassandraClient.SelectAllRecord(in.Exchange, in.Symbol, in.Period)
	if err != nil {
		return
	}

	records := make([]*pb.Record, 0)

	for _, r := range rs {
		records = append(
			records,
			&pb.Record{
				Exchange: r.Exchange,
				Symbol:   r.Symbol,
				Period:   r.Period,
				Datetime: datetime.GetTimestamp(r.Datetime),
				Name:     r.Name,
				Open:     r.Open.String(),
				High:     r.High.String(),
				Low:      r.Low.String(),
				Close:    r.Close.String(),
				Volume:   r.Volume,
			},
		)
	}

	out = &pb.GetSymbolReply{
		Records: records,
	}

	return
}
