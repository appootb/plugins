package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

type wrapper struct {
	pulsar.Client

	tenant    string
	namespace string
}
