package consumer

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	ch_consumer "github.com/veska-io/streams-connectors/consumers/clickhouse"
)

//go:embed sql/query.sql
var priceEventsTemplate string

func New(ctx context.Context, logger *slog.Logger, name, version string,
	host string, port uint32, database, username, password string,
	fromDate, toDate time.Time,
) (*ch_consumer.Consumer, error) {
	q, err := BuildEventsQuery()
	if err != nil {
		return nil, err
	}

	params := []any{
		clickhouse.Named("fromTimestamp", uint64(fromDate.UnixMilli())),
		clickhouse.Named("toTimestamp", uint64(toDate.UnixMilli())),
	}

	return ch_consumer.New(
		ctx, logger, name, version,
		host, port, database, username, password,
		q, params,
	)
}

func BuildEventsQuery() (string, error) {
	var rawTmpl bytes.Buffer

	tmpl, err := template.New("query.sql").Parse(priceEventsTemplate)
	if err != nil {
		return *new(string), fmt.Errorf("unable to parse template: %w", err)
	}

	err = tmpl.Execute(&rawTmpl, struct{}{})
	if err != nil {
		return *new(string), fmt.Errorf("unable to execute template: %w", err)
	}

	return rawTmpl.String(), nil
}
