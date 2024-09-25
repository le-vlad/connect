// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

func redpandaOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Summary("TODO").
		Fields(kafka.FranzWriterConfigFields()...).
		Fields(
			service.NewIntField(roFieldMaxInFlight).
				Description("The maximum number of batches to be sending in parallel at any given time.").
				Default(10),
			service.NewBatchPolicyField(roFieldBatching),
		).
		LintRule(`
root = if this.partitioner == "manual" {
if this.partition.or("") == "" {
"a partition must be specified when the partitioner is set to manual"
}
} else if this.partition.or("") != "" {
"a partition cannot be specified unless the partitioner is set to manual"
}`).
		Example("Simple Output", "Data is read from a topic foo and written to a topic bar, targetting the cluster configured within the redpanda block at the bottom. This is useful as it allows us to configured TLS and SASL only once for multiple inputs and outputs.", `
input:
  redpanda:
    topics: [ foo ]

pipeline:
  processors:
    - mutation: |
        root.id = uuid_v4()
        root.content = this.content.uppercase()

output:
  redpanda:
    topic: bar
    key: ${! @id }

redpanda:
  seed_brokers: [ "127.0.0.1:9093" ]
  tls:
    enabled: true
  sasl:
    - mechanism: SCRAM-SHA-512
      password: bar
      username: foo
`)
}

const (
	roFieldMaxInFlight = "max_in_flight"
	roFieldBatching    = "batching"
)

func init() {
	err := service.RegisterBatchOutput("redpanda", redpandaOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt(roFieldMaxInFlight); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(roFieldBatching); err != nil {
				return
			}
			output, err = kafka.NewFranzWriterFromConfig(conf, func(fn kafka.AccessClientFn) error {
				return useSharedClient(sharedGlobalRedpandaClientKey, mgr, fn)
			})
			return
		})
	if err != nil {
		panic(err)
	}
}
