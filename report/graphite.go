package report

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/foursquare/go-metrics"
)

type GraphiteConfig struct {
	Addr          *net.TCPAddr  // Network address to connect to
	FlushInterval time.Duration // Flush interval
}

func exporter(r *Recorder, c *GraphiteConfig) {
	for _ = range time.Tick(c.FlushInterval) {
		if err := sendToGraphite(r, c); nil != err {
			log.Println(err)
		}
	}
}

func sendToGraphite(r *Recorder, c *GraphiteConfig) error {
	conn, err := net.DialTCP("tcp", nil, c.Addr)
	if nil != err {
		return err
	}
	defer conn.Close()

	w := bufio.NewWriter(conn)
	writeStats(r, w)
	w.Flush()

	return nil
}

func writeStats(r *Recorder, w io.Writer) {
	now := time.Now().Unix()
	du := float64(r.DurationUnit)
	r.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			fmt.Fprintf(w, r.Format.Counter, r.Prefix, name, metric.Count(), now)
		case metrics.Gauge:
			fmt.Fprintf(w, r.Format.Gauge, r.Prefix, name, metric.Value(), now)
		case metrics.GaugeFloat64:
			fmt.Fprintf(w, r.Format.GaugeFloat64, r.Prefix, name, metric.Value(), now)
		case metrics.Histogram:
			h := metric.Snapshot()
			ps := h.Percentiles(r.Percentiles)
			fmt.Fprintf(w, r.Format.HistogramCount, r.Prefix, name, h.Count(), now)
			fmt.Fprintf(w, r.Format.Min, r.Prefix, name, h.Min(), now)
			fmt.Fprintf(w, r.Format.Max, r.Prefix, name, h.Max(), now)
			fmt.Fprintf(w, r.Format.Mean, r.Prefix, name, h.Mean(), now)
			fmt.Fprintf(w, r.Format.Stddev, r.Prefix, name, h.StdDev(), now)
			for psIdx, psKey := range r.Percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				fmt.Fprintf(w, r.Format.Percentile, r.Prefix, name, key, ps[psIdx], now)
			}
		case metrics.Meter:
			m := metric.Snapshot()
			fmt.Fprintf(w, r.Format.HistogramCount, r.Prefix, name, m.Count(), now)
			fmt.Fprintf(w, r.Format.Rate1, r.Prefix, name, m.Rate1(), now)
			fmt.Fprintf(w, r.Format.Rate5, r.Prefix, name, m.Rate5(), now)
			fmt.Fprintf(w, r.Format.Rate15, r.Prefix, name, m.Rate15(), now)
			fmt.Fprintf(w, r.Format.Mean, r.Prefix, name, m.RateMean(), now)
		case metrics.Timer:
			t := metric.Snapshot()
			ps := t.Percentiles(r.Percentiles)
			fmt.Fprintf(w, r.Format.HistogramCount, r.Prefix, name, t.Count(), now)
			fmt.Fprintf(w, r.Format.Min, r.Prefix, name, t.Min()/int64(du), now)
			fmt.Fprintf(w, r.Format.Max, r.Prefix, name, t.Max()/int64(du), now)
			fmt.Fprintf(w, r.Format.Mean, r.Prefix, name, t.Mean()/du, now)
			fmt.Fprintf(w, r.Format.Stddev, r.Prefix, name, t.StdDev()/du, now)
			for psIdx, psKey := range r.Percentiles {
				key := strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)
				fmt.Fprintf(w, r.Format.Percentile, r.Prefix, name, key, ps[psIdx]/du, now)
			}
			fmt.Fprintf(w, r.Format.Rate1, r.Prefix, name, t.Rate1(), now)
			fmt.Fprintf(w, r.Format.Rate5, r.Prefix, name, t.Rate5(), now)
			fmt.Fprintf(w, r.Format.Rate15, r.Prefix, name, t.Rate15(), now)
			fmt.Fprintf(w, r.Format.Mean, r.Prefix, name, t.RateMean(), now)
		default:
			log.Printf("Cannot export unknown metric type %T for '%s'\n", i, name)
		}
	})
}
