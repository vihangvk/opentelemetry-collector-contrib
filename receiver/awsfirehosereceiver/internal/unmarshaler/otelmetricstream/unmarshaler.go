// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelmetricstream // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler/otelmetricstream"

import (
	"errors"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.uber.org/zap"

	"github.com/gogo/protobuf/proto"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awsfirehosereceiver/internal/unmarshaler"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
)

const (
	// Supported version depends on version of go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp dependency
	TypeStr = "otel_v1"
)

var (
	errInvalidRecords         = errors.New("record format invalid")
	errInvalidOtelFormatStart = errors.New("unable to decode integer from OpenTelemetry message")
)

// Unmarshaler for the CloudWatch Metric Stream OpenTelemetry record format.
//
// More details can be found at:
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-metric-streams-formats-opentelemetry-100.html
type Unmarshaler struct {
	logger *zap.Logger
}

var _ unmarshaler.MetricsUnmarshaler = (*Unmarshaler)(nil)

// NewUnmarshaler creates a new instance of the Unmarshaler.
func NewUnmarshaler(logger *zap.Logger) *Unmarshaler {
	return &Unmarshaler{logger}
}

// Unmarshal deserializes the records into pmetric.Metrics
func (u Unmarshaler) Unmarshal(records [][]byte) (pmetric.Metrics, error) {
	md := pmetric.NewMetrics()
	for recordIndex, record := range records {
		var dataLen, pos = len(record), 0
		for pos < dataLen {
			n, nLen := proto.DecodeVarint(record)
			if nLen == 0 && pos == 0 {
				return md, errInvalidOtelFormatStart
			}
			req := pmetricotlp.NewExportRequest()
			err := req.UnmarshalProto(record[pos+nLen : pos+nLen+int(n)])
			pos += int(n) + nLen
			if err != nil {
				u.logger.Error(
					"Unable to unmarshal input",
					zap.Error(err),
					zap.Int("record_index", recordIndex),
				)
				continue
			}
			if !u.isValid(req.Metrics()) {
				u.logger.Error(
					"Invalid metric",
					zap.Int("record_index", recordIndex),
				)
				continue
			}
			req.Metrics().ResourceMetrics().MoveAndAppendTo(md.ResourceMetrics())
		}
	}

	if md.MetricCount() == 0 {
		return md, errInvalidRecords
	}
	return md, nil
}

// isValid validates that the metric has been unmarshalled correctly.
func (u Unmarshaler) isValid(metrics pmetric.Metrics) bool {
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		val, ok := metrics.ResourceMetrics().At(i).Resource().Attributes().Get(conventions.AttributeCloudProvider)
		if !ok || val.AsString() != conventions.AttributeCloudProviderAWS {
			return false
		}
	}
	return metrics.MetricCount() > 0
}

// Type of the serialized messages.
func (u Unmarshaler) Type() string {
	return TypeStr
}
