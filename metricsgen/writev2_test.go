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
