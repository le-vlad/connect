// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	rwFieldTopic           = "topic"
	rwFieldKey             = "key"
	rwFieldPartitioner     = "partitioner"
	rwFieldPartition       = "partition"
	rwFieldRackID          = "rack_id"
	rwFieldIdempotentWrite = "idempotent_write"
	rwFieldMetadata        = "metadata"
	rwFieldTimeout         = "timeout"
	rwFieldCompression     = "compression"
	rwFieldTimestamp       = "timestamp"
)

// FranzWriterConfigFields returns a slice of config fields specifically for
// customising data written to a Kafka broker.
func FranzWriterConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewInterpolatedStringField(rwFieldTopic).
			Description("A topic to write messages to."),
		service.NewInterpolatedStringField(rwFieldKey).
			Description("An optional key to populate for each message.").Optional(),
		service.NewStringAnnotatedEnumField(rwFieldPartitioner, map[string]string{
			"murmur2_hash": "Kafka's default hash algorithm that uses a 32-bit murmur2 hash of the key to compute which partition the record will be on.",
			"round_robin":  "Round-robin's messages through all available partitions. This algorithm has lower throughput and causes higher CPU load on brokers, but can be useful if you want to ensure an even distribution of records to partitions.",
			"least_backup": "Chooses the least backed up partition (the partition with the fewest amount of buffered records). Partitions are selected per batch.",
			"manual":       "Manually select a partition for each message, requires the field `partition` to be specified.",
		}).
			Description("Override the default murmur2 hashing partitioner.").
			Advanced().Optional(),
		service.NewInterpolatedStringField(rwFieldPartition).
			Description("An optional explicit partition to set for each message. This field is only relevant when the `partitioner` is set to `manual`. The provided interpolation string must be a valid integer.").
			Example(`${! meta("partition") }`).
			Optional(),
		service.NewStringField(rwFieldRackID).
			Description("A rack identifier for this client.").
			Default("").
			Advanced(),
		service.NewBoolField(rwFieldIdempotentWrite).
			Description("Enable the idempotent write producer option. This requires the `IDEMPOTENT_WRITE` permission on `CLUSTER` and can be disabled if this permission is not available.").
			Default(true).
			Advanced(),
		service.NewMetadataFilterField(rwFieldMetadata).
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional(),
		service.NewDurationField(rwFieldTimeout).
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced(),
		service.NewStringEnumField(rwFieldCompression, "lz4", "snappy", "gzip", "none", "zstd").
			Description("Optionally set an explicit compression type. The default preference is to use snappy when the broker supports it, and fall back to none if not.").
			Optional().
			Advanced(),
		service.NewInterpolatedStringField(rwFieldTimestamp).
			Description("An optional timestamp to set for each message. When left empty, the current timestamp is used.").
			Example(`${! timestamp_unix() }`).
			Example(`${! metadata("kafka_timestamp_unix") }`).
			Optional().
			Advanced(),
	}
}

// AccessClientFn defines a closure that receives a kafka client and returns an
// error.
type AccessClientFn func(client *kgo.Client) error

// FranzWriter implements a Kafka writer using the franz-go library.
type FranzWriter struct {
	topic            *service.InterpolatedString
	key              *service.InterpolatedString
	partition        *service.InterpolatedString
	timestamp        *service.InterpolatedString
	rackID           string
	idempotentWrite  bool
	metaFilter       *service.MetadataFilter
	partitioner      kgo.Partitioner
	timeout          time.Duration
	compressionPrefs []kgo.CompressionCodec

	accessClientFn func(AccessClientFn) error
}

// NewFranzWriterFromConfig uses a parsed config to extract customisation for
// writing data to a Kafka broker. A closure function must be provided that is
// responsible for granting access to a connected client.
func NewFranzWriterFromConfig(conf *service.ParsedConfig, accessClientFn func(AccessClientFn) error) (*FranzWriter, error) {
	w := FranzWriter{
		accessClientFn: accessClientFn,
	}

	var err error
	if w.topic, err = conf.FieldInterpolatedString(rwFieldTopic); err != nil {
		return nil, err
	}

	if conf.Contains(rwFieldKey) {
		if w.key, err = conf.FieldInterpolatedString(rwFieldKey); err != nil {
			return nil, err
		}
	}

	if rawStr, _ := conf.FieldString(rwFieldPartition); rawStr != "" {
		if w.partition, err = conf.FieldInterpolatedString(rwFieldPartition); err != nil {
			return nil, err
		}
	}

	if w.timeout, err = conf.FieldDuration(rwFieldTimeout); err != nil {
		return nil, err
	}

	if conf.Contains(rwFieldCompression) {
		cStr, err := conf.FieldString(rwFieldCompression)
		if err != nil {
			return nil, err
		}

		var c kgo.CompressionCodec
		switch cStr {
		case "lz4":
			c = kgo.Lz4Compression()
		case "gzip":
			c = kgo.GzipCompression()
		case "snappy":
			c = kgo.SnappyCompression()
		case "zstd":
			c = kgo.ZstdCompression()
		case "none":
			c = kgo.NoCompression()
		default:
			return nil, fmt.Errorf("compression codec %v not recognised", cStr)
		}
		w.compressionPrefs = append(w.compressionPrefs, c)
	}

	w.partitioner = kgo.StickyKeyPartitioner(nil)
	if conf.Contains(rwFieldPartitioner) {
		partStr, err := conf.FieldString(rwFieldPartitioner)
		if err != nil {
			return nil, err
		}
		switch partStr {
		case "murmur2_hash":
			w.partitioner = kgo.StickyKeyPartitioner(nil)
		case "round_robin":
			w.partitioner = kgo.RoundRobinPartitioner()
		case "least_backup":
			w.partitioner = kgo.LeastBackupPartitioner()
		case "manual":
			w.partitioner = kgo.ManualPartitioner()
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
		}
	}

	if w.rackID, err = conf.FieldString(rwFieldRackID); err != nil {
		return nil, err
	}

	if w.idempotentWrite, err = conf.FieldBool(rwFieldIdempotentWrite); err != nil {
		return nil, err
	}

	if conf.Contains(rwFieldMetadata) {
		if w.metaFilter, err = conf.FieldMetadataFilter(rwFieldMetadata); err != nil {
			return nil, err
		}
	}

	if conf.Contains(rwFieldTimestamp) {
		if w.timestamp, err = conf.FieldInterpolatedString(rwFieldTimestamp); err != nil {
			return nil, err
		}
	}
	return &w, nil
}

//------------------------------------------------------------------------------

// Connect to the target seed brokers.
func (w *FranzWriter) Connect(ctx context.Context) error {
	return w.accessClientFn(func(client *kgo.Client) error {
		// Check connectivity to cluster
		if err := client.Ping(ctx); err != nil {
			return fmt.Errorf("failed to connect to cluster: %s", err)
		}
		return nil
	})
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (w *FranzWriter) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	return w.accessClientFn(func(client *kgo.Client) (err error) {
		records := make([]*kgo.Record, 0, len(b))
		for i, msg := range b {
			var topic string
			if topic, err = b.TryInterpolatedString(i, w.topic); err != nil {
				return fmt.Errorf("topic interpolation error: %w", err)
			}

			record := &kgo.Record{Topic: topic}
			if record.Value, err = msg.AsBytes(); err != nil {
				return
			}
			if w.key != nil {
				if record.Key, err = b.TryInterpolatedBytes(i, w.key); err != nil {
					return fmt.Errorf("key interpolation error: %w", err)
				}
			}
			if w.partition != nil {
				partStr, err := b.TryInterpolatedString(i, w.partition)
				if err != nil {
					return fmt.Errorf("partition interpolation error: %w", err)
				}
				partInt, err := strconv.Atoi(partStr)
				if err != nil {
					return fmt.Errorf("partition parse error: %w", err)
				}
				record.Partition = int32(partInt)
			}
			_ = w.metaFilter.Walk(msg, func(key, value string) error {
				record.Headers = append(record.Headers, kgo.RecordHeader{
					Key:   key,
					Value: []byte(value),
				})
				return nil
			})
			if w.timestamp != nil {
				if tsStr, err := b.TryInterpolatedString(i, w.timestamp); err != nil {
					return fmt.Errorf("timestamp interpolation error: %w", err)
				} else {
					if ts, err := strconv.ParseInt(tsStr, 10, 64); err != nil {
						return fmt.Errorf("failed to parse timestamp: %w", err)
					} else {
						record.Timestamp = time.Unix(ts, 0)
					}
				}
			}
			records = append(records, record)
		}

		// TODO: This is very cool and allows us to easily return granular errors,
		// so we should honor travis by doing it.
		return client.ProduceSync(ctx, records...).FirstErr()
	})
}

// Close does nothing as this component doesn't own the client.
func (w *FranzWriter) Close(ctx context.Context) error {
	return nil
}
