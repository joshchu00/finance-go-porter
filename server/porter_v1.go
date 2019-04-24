package server

import (
	"context"
	"fmt"
	"sort"

	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/logger"
	pb "github.com/joshchu00/finance-protobuf/porter"
)

type PorterV1Server struct {
	CassandraClient *cassandra.Client
}

func (s *PorterV1Server) GetSymbol(ctx context.Context, in *pb.GetSymbolRequest) (out *pb.GetSymbolReply, err error) {

	logger.Info(fmt.Sprintf("%s: %s %s %s", "GetSymbol", in.Exchange, in.Symbol, in.Period))

	tickMap := make(map[int64]*pb.Tick, 0)

	var name string

	// Record
	var rrs []*cassandra.RecordRow
	rrs, err = s.CassandraClient.SelectRecordRowsByPartitionKey(
		&cassandra.RecordPartitionKey{
			Exchange: in.Exchange,
			Symbol:   in.Symbol,
			Period:   in.Period,
		},
	)
	if err != nil {
		return
	}

	for i, rr := range rrs {

		if i == len(rrs)-1 {
			name = rr.Name
		}

		ts := datetime.GetTimestamp(rr.Datetime)

		tick, ok := tickMap[ts]
		if !ok {
			tick = &pb.Tick{
				Datetime: ts,
			}
			tickMap[ts] = tick
		}

		tick.Record = &pb.Record{
			Open:   rr.Open.String(),
			High:   rr.High.String(),
			Low:    rr.Low.String(),
			Close:  rr.Close.String(),
			Volume: rr.Volume,
		}
	}

	// Indicator
	var irs []*cassandra.IndicatorRow
	irs, err = s.CassandraClient.SelectIndicatorRowsByPartitionKey(
		&cassandra.IndicatorPartitionKey{
			Exchange: in.Exchange,
			Symbol:   in.Symbol,
			Period:   in.Period,
		},
	)
	if err != nil {
		return
	}

	for _, ir := range irs {

		ts := datetime.GetTimestamp(ir.Datetime)

		tick, ok := tickMap[ts]
		if !ok {
			logger.Warn(fmt.Sprintf("%s: %s %s %s %d", "Unknown indicator", in.Exchange, in.Symbol, in.Period, ts))
			tick = &pb.Tick{
				Datetime: ts,
			}
			tickMap[ts] = tick
		}

		tick.Indicator = &pb.Indicator{
			Sma0005: ir.SMA0005.String(),
			Sma0010: ir.SMA0010.String(),
			Sma0020: ir.SMA0020.String(),
			Sma0060: ir.SMA0060.String(),
			Sma0120: ir.SMA0120.String(),
			Sma0240: ir.SMA0240.String(),
		}
	}

	// Strategy
	var srs []*cassandra.StrategyRow
	srs, err = s.CassandraClient.SelectStrategyRowsByPartitionKey(
		&cassandra.StrategyPartitionKey{
			Exchange: in.Exchange,
			Symbol:   in.Symbol,
			Period:   in.Period,
		},
	)
	if err != nil {
		return
	}

	for _, sr := range srs {

		ts := datetime.GetTimestamp(sr.Datetime)

		tick, ok := tickMap[ts]
		if !ok {
			logger.Warn(fmt.Sprintf("%s: %s %s %s %d", "Unknown strategy", in.Exchange, in.Symbol, in.Period, ts))
			tick = &pb.Tick{
				Datetime: ts,
			}
			tickMap[ts] = tick
		}

		tick.Strategy = &pb.Strategy{
			Ssma: sr.SSMA,
			Lsma: sr.LSMA,
		}
	}

	// Sort by ts
	tickTSs := make([]int64, 0, len(tickMap))
	for tickTS := range tickMap {
		tickTSs = append(tickTSs, tickTS)
	}

	sort.Slice(tickTSs, func(i, j int) bool { return tickTSs[i] < tickTSs[j] })

	ticks := make([]*pb.Tick, 0)
	for _, tickTS := range tickTSs {
		ticks = append(ticks, tickMap[tickTS])
	}

	out = &pb.GetSymbolReply{
		Exchange: in.Exchange,
		Symbol:   in.Symbol,
		Period:   in.Period,
		Name:     name,
		Ticks:    ticks,
	}

	return
}
