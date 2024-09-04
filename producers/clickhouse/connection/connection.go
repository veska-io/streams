package connection

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func New(ctx context.Context, logger *slog.Logger,
	host, database, username, password string,
) (driver.Conn, error) {
	var (
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", host, 9440)},
			Auth: clickhouse.Auth{
				Database: database,
				Username: username,
				Password: password,
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "Streams Conector Producer", Version: "v0.0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				logger.Debug("clickhouse debug", slog.String("format", fmt.Sprintf(format, v)))
			},
			TLS: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			logger.Error("clickhouse exception",
				slog.Int("code", int(exception.Code)),
				slog.String("message", exception.Message),
				slog.String("stack_trace", exception.StackTrace),
			)
		}
		return nil, err
	}
	return conn, nil
}
