// Copyright 2022 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metricsgen

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/exp/api/remote"
	writev2 "github.com/prometheus/client_golang/exp/api/remote/genproto/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func (w *Writer) writeV2(ctx context.Context) error {
	select {
	// Wait for update first as write and collector.Run runs simultaneously.
	case <-w.config.UpdateNotify:
	case <-ctx.Done():
		return ctx.Err()
	}

	series, err := collectMetricsV2(w.gatherer, w.config.OutOfOrder)
	if err != nil {
		return err
	}

	var (
		totalTime       time.Duration
		totalSamplesExp = len(series) * w.config.RequestCount
		totalSamplesAct int
		stats           remote.WriteResponseStats
		mtx             sync.Mutex
		wgMetrics       sync.WaitGroup
		merr            []error
	)

	shouldRunForever := w.config.RequestCount == -1
	if shouldRunForever {
		log.Printf("Sending: %v timeseries infinitely, %v timeseries per request, %v delay between requests\n",
			len(series), w.config.BatchSize, w.config.RequestInterval)
	} else {
		log.Printf("Sending: %v timeseries, %v times, %v timeseries per request, %v delay between requests\n",
			len(series), w.config.RequestCount, w.config.BatchSize, w.config.RequestInterval)
	}

	ticker := time.NewTicker(w.config.RequestInterval)
	defer ticker.Stop()

	concurrencyLimitCh := make(chan struct{}, w.config.Concurrency)

	for i := 0; ; {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !shouldRunForever {
			if i >= w.config.RequestCount {
				break
			}
			i++
		}

		<-ticker.C
		select {
		case <-w.config.UpdateNotify:
			log.Println("updating remote write metrics")
			series, err = collectMetricsV2(w.gatherer, w.config.OutOfOrder)
			if err != nil {
				mtx.Lock()
				merr = append(merr, err)
				mtx.Unlock()
			}
		default:
			updateTimestampsV2(series)
		}

		start := time.Now()
		for i := 0; i < len(series); i += w.config.BatchSize {
			wgMetrics.Add(1)
			concurrencyLimitCh <- struct{}{}
			go func(i int) {
				defer func() {
					<-concurrencyLimitCh
				}()
				defer wgMetrics.Done()
				end := i + w.config.BatchSize
				if end > len(series) {
					end = len(series)
				}
				req := buildV2Request(series[i:end])

				st, err := w.remoteAPI.Write(ctx, remote.WriteV2MessageType, req)
				if err != nil {
					mtx.Lock()
					merr = append(merr, err)
					mtx.Unlock()
					w.logger.Error("error writing metrics", "error", err)
					return
				}

				mtx.Lock()
				totalSamplesAct += len(req.Timeseries)
				stats.Add(st)
				mtx.Unlock()
			}(i)
		}
		wgMetrics.Wait()
		totalTime += time.Since(start)
		if countErrors(merr) > 20 {
			merr = append(merr, errors.New("too many errors"))
			return errors.Join(merr...)
		}
	}
	if w.config.RequestCount*len(series) != totalSamplesAct {
		merr = append(merr, fmt.Errorf("total samples mismatch, exp:%v , act:%v", totalSamplesExp, totalSamplesAct))
	}
	w.logger.Info("metrics summary",
		"total_time", totalTime.Round(time.Second),
		"total_samples", totalSamplesAct,
		"samples_per_sec", int(float64(totalSamplesAct)/totalTime.Seconds()),
		"written_samples", stats.Samples,
		"written_histograms", stats.Histograms,
		"written_exemplars", stats.Exemplars,
		"errors", countErrors(merr))
	return errors.Join(merr...)
}

func countErrors(merr []error) int {
	count := 0
	for _, err := range merr {
		if err != nil {
			count++
		}
	}
	return count
}

func updateTimestampsV2(series []*v2Series) {
	now := time.Now().UnixMilli()
	for _, s := range series {
		s.timestamp = now
	}
}

func shuffleTimestampsV2(series []*v2Series) {
	now := time.Now().UnixMilli()
	offsets := []int64{0, -60 * 1000, -5 * 60 * 1000}
	for i, s := range series {
		s.timestamp = now + offsets[i%len(offsets)]
	}
}

func collectMetricsV2(gatherer prometheus.Gatherer, outOfOrder bool) ([]*v2Series, error) {
	metricFamilies, err := gatherer.Gather()
	if err != nil {
		return nil, err
	}
	series := toV2Series(metricFamilies)
	if outOfOrder {
		shuffleTimestampsV2(series)
	}
	return series, nil
}

// v2Series is an intermediate, non-interned representation of a single
// remote write 2.0 series. Strings stay resolved until request assembly,
// where they are interned into a symbol table scoped to one request.
// Later increments add metadata, histograms, exemplars and created
// timestamps here.
type v2Series struct {
	labels    []string // flat name/value pairs, sorted by label name
	value     float64
	timestamp int64 // epoch milliseconds
}

// flatLabels converts a metric's labels plus __name__ into the flat,
// sorted name/value pair form SymbolsTable.SymbolizeLabels expects.
func flatLabels(name string, label []*dto.LabelPair) []string {
	lbls := prompbLabels(name, label)
	flat := make([]string, 0, len(lbls)*2)
	for _, l := range lbls {
		flat = append(flat, l.Name, l.Value)
	}
	return flat
}

// toV2Series converts gathered metric families into the intermediate
// series model. Only counters and gauges are implemented so far.
func toV2Series(metricFamilies []*dto.MetricFamily) []*v2Series {
	timestamp := time.Now().UnixMilli()
	series := make([]*v2Series, 0, len(metricFamilies)*10)

	skippedSamples := 0
	for _, metricFamily := range metricFamilies {
		for _, metric := range metricFamily.Metric {
			s := &v2Series{
				labels:    flatLabels(*metricFamily.Name, metric.Label),
				timestamp: timestamp,
			}
			switch *metricFamily.Type {
			case dto.MetricType_COUNTER:
				s.value = *metric.Counter.Value
			case dto.MetricType_GAUGE:
				s.value = *metric.Gauge.Value
			default:
				skippedSamples++
				continue
			}
			series = append(series, s)
		}
	}
	if skippedSamples > 0 {
		log.Printf("WARN: Skipping %v samples; sending only %v samples, given only gauge and counters are currently implemented\n", skippedSamples, len(series))
	}
	return series
}

// buildV2Request assembles one remote write 2.0 request, interning every
// string into a symbol table scoped to this request only, as real senders
// do (the receiver deduplicates per request).
func buildV2Request(series []*v2Series) *writev2.Request {
	st := writev2.NewSymbolTable()
	tss := make([]*writev2.TimeSeries, 0, len(series))
	for _, s := range series {
		tss = append(tss, &writev2.TimeSeries{
			LabelsRefs: st.SymbolizeLabels(s.labels, nil),
			Samples: []*writev2.Sample{{
				Value:     s.value,
				Timestamp: s.timestamp,
			}},
		})
	}
	return &writev2.Request{
		Symbols:    st.Symbols(),
		Timeseries: tss,
	}
}
