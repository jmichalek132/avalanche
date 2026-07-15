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
	"testing"

	writev2 "github.com/prometheus/client_golang/exp/api/remote/genproto/v2"
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
