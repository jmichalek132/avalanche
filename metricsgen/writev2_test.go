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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/client_golang/exp/api/remote"
	writev2 "github.com/prometheus/client_golang/exp/api/remote/genproto/v2"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func strPtr(s string) *string                   { return &s }
func f64Ptr(f float64) *float64                 { return &f }
func mtypePtr(m dto.MetricType) *dto.MetricType { return &m }

func TestToV2Series(t *testing.T) {
	families := []*dto.MetricFamily{
		{
			Name: strPtr("test_counter_total"),
			Type: mtypePtr(dto.MetricType_COUNTER),
			Metric: []*dto.Metric{{
				Label: []*dto.LabelPair{
					{Name: strPtr("series_id"), Value: strPtr("0")},
					{Name: strPtr("cycle_id"), Value: strPtr("1")},
				},
				Counter: &dto.Counter{Value: f64Ptr(42)},
			}},
		},
		{
			Name: strPtr("test_gauge"),
			Type: mtypePtr(dto.MetricType_GAUGE),
			Metric: []*dto.Metric{{
				Gauge: &dto.Gauge{Value: f64Ptr(7)},
			}},
		},
		{
			Name: strPtr("test_histogram"),
			Type: mtypePtr(dto.MetricType_HISTOGRAM),
			Metric: []*dto.Metric{{
				Histogram: &dto.Histogram{},
			}},
		},
	}

	series := toV2Series(families)

	// Histograms are still skipped in this PR (behavior unchanged).
	require.Len(t, series, 2)

	require.Equal(t,
		[]string{"__name__", "test_counter_total", "cycle_id", "1", "series_id", "0"},
		series[0].labels)
	require.Equal(t, 42.0, series[0].value)
	require.NotZero(t, series[0].timestamp)

	require.Equal(t, []string{"__name__", "test_gauge"}, series[1].labels)
	require.Equal(t, 7.0, series[1].value)
}

func TestBuildV2RequestSymbols(t *testing.T) {
	series := []*v2Series{
		{labels: []string{"__name__", "metric_a", "label", "value_a"}, value: 1, timestamp: 1000},
		{labels: []string{"__name__", "metric_b", "label", "value_b"}, value: 2, timestamp: 1000},
	}

	req := buildV2Request(series)

	require.Len(t, req.Timeseries, 2)
	require.NotEmpty(t, req.Symbols)
	require.Equal(t, "", req.Symbols[0], "symbols must start with the empty string")

	referenced := map[uint32]bool{}
	for i, ts := range req.Timeseries {
		require.Len(t, ts.LabelsRefs, len(series[i].labels))
		for _, ref := range ts.LabelsRefs {
			require.Less(t, int(ref), len(req.Symbols), "ref out of range")
			referenced[ref] = true
		}
		require.Equal(t, series[i].labels,
			writev2.DesymbolizeLabels(ts.LabelsRefs, req.Symbols, nil))

		require.Len(t, ts.Samples, 1)
		require.Equal(t, series[i].value, ts.Samples[0].Value)
		require.Equal(t, series[i].timestamp, ts.Samples[0].Timestamp)
	}

	// Per-request minimality: every non-empty symbol is referenced.
	for ref := 1; ref < len(req.Symbols); ref++ {
		require.True(t, referenced[uint32(ref)],
			"symbol %q is not referenced by this request", req.Symbols[ref])
	}
}

func TestBuildV2RequestScopedPerBatch(t *testing.T) {
	batchA := []*v2Series{{labels: []string{"__name__", "only_in_a"}, value: 1, timestamp: 1}}
	batchB := []*v2Series{{labels: []string{"__name__", "only_in_b"}, value: 2, timestamp: 1}}

	reqA := buildV2Request(batchA)
	reqB := buildV2Request(batchB)

	require.Contains(t, reqA.Symbols, "only_in_a")
	require.NotContains(t, reqA.Symbols, "only_in_b")
	require.Contains(t, reqB.Symbols, "only_in_b")
	require.NotContains(t, reqB.Symbols, "only_in_a")
}

func TestShuffleTimestampsV2(t *testing.T) {
	series := []*v2Series{{}, {}, {}}
	before := time.Now().UnixMilli()

	shuffleTimestampsV2(series)

	offsets := []int64{0, -60 * 1000, -5 * 60 * 1000}
	for i, s := range series {
		// Offsets are assigned round-robin by index.
		require.InDelta(t, offsets[i%len(offsets)], s.timestamp-before, 1000, "series %d", i)
	}

	outOfOrder := false
	for i := 1; i < len(series); i++ {
		if series[i].timestamp < series[i-1].timestamp {
			outOfOrder = true
		}
	}
	require.True(t, outOfOrder, "timestamps are not out of order")
}

func TestWriteV2EndToEnd(t *testing.T) {
	var (
		mtx      sync.Mutex
		captured []*writev2.Request
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		raw, err := snappy.Decode(nil, body)
		if err != nil {
			t.Errorf("snappy decode: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req := &writev2.Request{}
		if err := req.UnmarshalVT(raw); err != nil {
			t.Errorf("unmarshal: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		mtx.Lock()
		captured = append(captured, req)
		mtx.Unlock()

		w.Header().Set("X-Prometheus-Remote-Write-Samples-Written", strconv.Itoa(len(req.Timeseries)))
		w.Header().Set("X-Prometheus-Remote-Write-Histograms-Written", "0")
		w.Header().Set("X-Prometheus-Remote-Write-Exemplars-Written", "0")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	reg := prometheus.NewRegistry()
	for i := 0; i < 4; i++ {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: fmt.Sprintf("test_gauge_%d", i)})
		g.Set(float64(i))
		reg.MustRegister(g)
	}

	api, err := remote.NewAPI(srv.URL, remote.WithAPIHTTPClient(srv.Client()))
	require.NoError(t, err)

	updateNotify := make(chan struct{}, 1)
	updateNotify <- struct{}{}

	const requestCount = 3
	var logBuf bytes.Buffer
	writer := &Writer{
		logger: slog.New(slog.NewTextHandler(&logBuf, nil)),
		config: &ConfigWrite{
			RequestInterval: 10 * time.Millisecond,
			BatchSize:       2,
			RequestCount:    requestCount,
			UpdateNotify:    updateNotify,
			Concurrency:     2,
		},
		gatherer:  reg,
		remoteAPI: api,
	}

	require.NoError(t, writer.writeV2(context.Background()))

	// Receiver-confirmed stats accumulated across all requests:
	// requestCount ticks x 4 series = 12 written samples.
	require.Contains(t, logBuf.String(), "written_samples=12")

	mtx.Lock()
	defer mtx.Unlock()
	// 4 series / batch size 2 = 2 requests per tick, requestCount ticks.
	require.Len(t, captured, requestCount*2)

	allTimestamps := map[int64]bool{}
	for _, req := range captured {
		require.Len(t, req.Timeseries, 2)
		require.NotEmpty(t, req.Symbols)
		require.Equal(t, "", req.Symbols[0])

		reqTimestamps := map[int64]bool{}
		for _, ts := range req.Timeseries {
			for _, ref := range ts.LabelsRefs {
				require.Less(t, int(ref), len(req.Symbols))
			}
			labels := writev2.DesymbolizeLabels(ts.LabelsRefs, req.Symbols, nil)
			require.Equal(t, "__name__", labels[0])

			require.Len(t, ts.Samples, 1)
			reqTimestamps[ts.Samples[0].Timestamp] = true
			allTimestamps[ts.Samples[0].Timestamp] = true
		}
		require.Len(t, reqTimestamps, 1, "all samples in one request share the tick timestamp")
	}
	require.GreaterOrEqual(t, len(allTimestamps), 2, "timestamps advance across ticks")
}
