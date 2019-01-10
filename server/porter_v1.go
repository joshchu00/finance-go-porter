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

	var name string
	records := make([]*pb.Record, 0)

	for i, r := range rs {

		if i == len(rs)-1 {
			name = r.Name
		}

		records = append(
			records,
			&pb.Record{
				Datetime: datetime.GetTimestamp(r.Datetime),
				Open:     r.Open.String(),
				High:     r.High.String(),
				Low:      r.Low.String(),
				Close:    r.Close.String(),
				Volume:   r.Volume,
			},
		)
	}

	out = &pb.GetSymbolReply{
		Exchange: in.Exchange,
		Symbol:   in.Symbol,
		Period:   in.Period,
		Name:     name,
		Records:  records,
	}

	return
}
