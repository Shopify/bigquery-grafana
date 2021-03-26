package datasource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"google.golang.org/api/iterator"
)

type queryResult struct {
	Time   time.Time `bigquery:"time"`
	Values int64     `bigquery:"metric"`
}

// TransformedResults contains the results from BigQuery for Grafana
type TransformedResults struct {
	Time   []time.Time
	Values []int64
}

type queryModel struct {
	Format string `json:"format"`
	Dataset          string   `json:"dataset"`
	Group            []string `json:"group"`
	MetricColumn     string   `json:"metricColumn"`
	OrderByCol       string   `json:"orderByCol"`
	OrderBySort      string   `json:"orderBySort"`
	Partitioned      bool     `json:"partitioned"`
	PartitionedField string   `json:"partitionedField"`
	Project          string   `json:"project"`
	RawQuery         bool     `json:"rawQuery"`
	RawSQL           string   `json:"rawSql"`
	RefID            string   `json:"refId"`
	Sharded          bool     `json:"sharded"`
	Table            string   `json:"table"`
	TimeColumn       string   `json:"timeColumn"`
	TimeColumnType   string   `json:"timeColumnType"`
	Location         string   `json:"location"`
}

type instanceSettings struct {
	httpClient *http.Client
}

func newDataSourceInstance(_ backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	return &instanceSettings{
		httpClient: &http.Client{},
	}, nil
}

// Dispose is called before creating a new instance to allow plugin authors to cleanup
func (s *instanceSettings) Dispose() {}

// BigQueryDatasource is an example datasource used to scaffold
// new datasource plugins with an backend.
type BigQueryDatasource struct {
	log.Logger
	im instancemgmt.InstanceManager
}

// New creates a datasource that is sued for querying BigQuery
func New(logger log.Logger) datasource.ServeOpts {
	im := datasource.NewInstanceManager(newDataSourceInstance)
	ds := &BigQueryDatasource{
		Logger: logger,
		im: im,
	}

	return datasource.ServeOpts{
		QueryDataHandler:   ds,
		CheckHealthHandler: ds,
	}
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (bq *BigQueryDatasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()
	mtx := &sync.Mutex{}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	// Execute the queries in parallel and collect the responses.
	for _, q := range req.Queries {
		go func(dq backend.DataQuery) {
			res := bq.doQuery(ctx, dq)
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				mtx.Lock()
				response.Responses[dq.RefID] = res
				mtx.Unlock()
			}
		}(q)
	}

	return response, nil
}

func (bq *BigQueryDatasource) doQuery(ctx context.Context, query backend.DataQuery) backend.DataResponse {
	var qm queryModel
	var response backend.DataResponse

	response.Error = json.Unmarshal(query.JSON, &qm)
	if response.Error != nil {
		return response
	}

	rows, err := bq.runQuery(ctx, qm)
	if err != nil {
		bq.Error("Error running query: %v", err)
		response.Error = err
		return response
	}
	if qm.Format == "" {
		bq.Warn("Format is empty. Defaulting to time series")
	}

	frame := data.NewFrame("response")
	frame.Fields = append(frame.Fields,
		data.NewField("Time", nil, rows.Time),
		data.NewField("Values", nil, rows.Values),
	)

	response.Frames = append(response.Frames, frame)
	return response
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (bq *BigQueryDatasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	var status = backend.HealthStatusOk
	var message = "Data source is working"

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

// BigQueryRun runs the query against BigQuery
func (bq *BigQueryDatasource) runQuery(ctx context.Context, query queryModel) (*TransformedResults, error) {
	projectID := query.Project
	var tr TransformedResults

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("couldn't create BigQuery client: %w", err)
	}
	defer client.Close()

	q := client.Query(query.RawSQL)
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = query.Location

	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("waiting for query: %w", err)
	}
	if err = status.Err(); err != nil {
		return nil, fmt.Errorf("query returned error: %w", err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading job: %w", err)
	}

	for {
		var row queryResult
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterating rows in job: %w", err)
		}
		log.DefaultLogger.Info("Rows", "Query", row)

		tr.Time = append(tr.Time, row.Time)
		tr.Values = append(tr.Values, row.Values)
	}

	return &tr, nil
}
