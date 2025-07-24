module server

go 1.22.0

require github.com/lordbasex/burrowctl v0.0.0

replace github.com/lordbasex/burrowctl => ../../

require (
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
)
