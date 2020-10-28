module github.com/appootb/plugins

go 1.14

require (
	github.com/Shopify/sarama v1.27.2
	github.com/appootb/grc v0.0.0-20201016070053-996c36bb7e8e
	github.com/appootb/protobuf/go v0.0.0-20201019071631-0aacfbff06fa
	github.com/appootb/substratum v0.0.0-20201026044318-a2dec025366f
	github.com/go-redis/redis/v8 v8.2.3
)

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0
