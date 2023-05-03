module github.com/dtm-labs/client

go 1.16

require (
	github.com/dtm-labs/dtmdriver v0.0.6
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-resty/resty/v2 v2.7.0
	go.mongodb.org/mongo-driver v1.9.1
	google.golang.org/grpc v1.54.0
	google.golang.org/protobuf v1.30.0
)

require (
	github.com/dtm-labs/logger v0.0.1
	github.com/uptrace/opentelemetry-go-extra/otelsql v0.1.21
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.28.0
	go.opentelemetry.io/otel v1.14.0
)

retract v1.18.7
