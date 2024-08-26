package consumer

import (
	"context"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/veska-io/streams-connectors/consumers/clickhouse/connection"
)

type Consumer struct {
	ctx    context.Context
	logger *slog.Logger

	conn     driver.Conn
	host     string
	database string
	username string
	password string

	query  string
	params []any

	DataStream chan *driver.Rows
}

func New(ctx context.Context, logger *slog.Logger, name, version string,
	host string, port uint32, database, username, password string,
	query string, params []any,
) (*Consumer, error) {
	conn, err := connection.New(
		ctx, logger,
		host, port, database, username, password, name, version,
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		ctx:    ctx,
		logger: logger,

		conn: conn,

		host:     host,
		database: database,
		username: username,
		password: password,

		query:  query,
		params: params,

		DataStream: make(chan *driver.Rows),
	}, nil
}

func (c *Consumer) Run() {
	rows, err := c.conn.Query(c.ctx, c.query, c.params...)

	if err != nil {
		c.logger.Error("clickhouse query failed", slog.String("err", err.Error()))
	}

	for rows.Next() {
		c.DataStream <- &rows
	}

	rows.Close()
	close(c.DataStream)
}
