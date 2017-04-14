package bigquery

import (
	"golang.org/x/net/context"

	"google.golang.org/appengine/log"

	bq "cloud.google.com/go/bigquery"
)

// LoadConfigForCSV is CSV File Load Config
type LoadConfigForCSV struct {
	FieldDelimiter      string
	SkipLeadingRows     int64
	AllowJaggedRows     bool
	AllowQuotedNewlines bool
	Encoding            bq.Encoding
	MaxBadRecords       int64
	IgnoreUnknownValues bool
	Schema              bq.Schema
	Quote               string
	ForceZeroQuote      bool
	CreateDisposition   bq.TableCreateDisposition
	WriteDisposition    bq.TableWriteDisposition
}

// LoadFromCSV is Cloud Storage Object to Table
func LoadFromCSV(c context.Context, projectID, datasetID string, tableID string, gcsPath string, config *LoadConfigForCSV) (*bq.Job, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}

	gcsRef := bq.NewGCSReference(gcsPath)
	gcsRef.SourceFormat = bq.CSV
	gcsRef.FieldDelimiter = config.FieldDelimiter
	gcsRef.SkipLeadingRows = config.SkipLeadingRows
	gcsRef.AllowJaggedRows = config.AllowJaggedRows
	gcsRef.AllowQuotedNewlines = config.AllowQuotedNewlines
	gcsRef.Encoding = config.Encoding
	gcsRef.MaxBadRecords = config.MaxBadRecords
	gcsRef.IgnoreUnknownValues = config.IgnoreUnknownValues
	gcsRef.Schema = config.Schema
	gcsRef.Quote = config.Quote
	gcsRef.ForceZeroQuote = config.ForceZeroQuote

	d := client.Dataset(datasetID)
	loader := d.Table(tableID).LoaderFrom(gcsRef)
	loader.CreateDisposition = config.CreateDisposition
	loader.WriteDisposition = config.WriteDisposition
	j, err := loader.Run(c)
	if err != nil {
		return nil, err
	}

	return j, nil
}

// LoadConfigForJSON is JSON File Load Config
type LoadConfigForJSON struct {
	Encoding            bq.Encoding
	MaxBadRecords       int64
	IgnoreUnknownValues bool
	Schema              bq.Schema
	Quote               string
	ForceZeroQuote      bool
	CreateDisposition   bq.TableCreateDisposition
	WriteDisposition    bq.TableWriteDisposition
}

// LoadFromJSON is Cloud Storage Object to Table
func LoadFromJSON(c context.Context, projectID, datasetID string, tableID string, gcsPath string, config *LoadConfigForJSON) (*bq.Job, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}

	gcsRef := bq.NewGCSReference(gcsPath)
	gcsRef.SourceFormat = bq.JSON
	gcsRef.Encoding = config.Encoding
	gcsRef.MaxBadRecords = config.MaxBadRecords
	gcsRef.IgnoreUnknownValues = config.IgnoreUnknownValues
	gcsRef.Schema = config.Schema
	gcsRef.Quote = config.Quote
	gcsRef.ForceZeroQuote = config.ForceZeroQuote

	d := client.Dataset(datasetID)
	loader := d.Table(tableID).LoaderFrom(gcsRef)
	loader.CreateDisposition = config.CreateDisposition
	loader.WriteDisposition = config.WriteDisposition
	j, err := loader.Run(c)
	if err != nil {
		return nil, err
	}

	return j, nil
}

// LoadConfigForDatastoreBackup is Datastore Backup File Load Config
type LoadConfigForDatastoreBackup struct {
	MaxBadRecords     int64
	CreateDisposition bq.TableCreateDisposition
	WriteDisposition  bq.TableWriteDisposition
}

// LoadFromDatastoreBackup is Cloud Storage Object to Table
func LoadFromDatastoreBackup(c context.Context, projectID, datasetID string, tableID string, gcsPath string, config *LoadConfigForDatastoreBackup) (*bq.Job, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}

	gcsRef := bq.NewGCSReference(gcsPath)
	gcsRef.SourceFormat = bq.DatastoreBackup
	gcsRef.MaxBadRecords = config.MaxBadRecords

	d := client.Dataset(datasetID)
	loader := d.Table(tableID).LoaderFrom(gcsRef)
	loader.CreateDisposition = config.CreateDisposition
	loader.WriteDisposition = config.WriteDisposition
	j, err := loader.Run(c)
	if err != nil {
		return nil, err
	}

	return j, nil
}

// LoadConfigForAvro is Avro File Load Config
type LoadConfigForAvro struct {
	MaxBadRecords     int64
	CreateDisposition bq.TableCreateDisposition
	WriteDisposition  bq.TableWriteDisposition
}

// LoadFromAvro is Cloud Storage Object to Table
func LoadFromAvro(c context.Context, projectID, datasetID string, tableID string, gcsPath string, config *LoadConfigForAvro) (*bq.Job, error) {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return nil, err
	}

	gcsRef := bq.NewGCSReference(gcsPath)
	gcsRef.SourceFormat = bq.Avro
	gcsRef.MaxBadRecords = config.MaxBadRecords

	d := client.Dataset(datasetID)
	loader := d.Table(tableID).LoaderFrom(gcsRef)
	loader.CreateDisposition = config.CreateDisposition
	loader.WriteDisposition = config.WriteDisposition
	j, err := loader.Run(c)
	if err != nil {
		return nil, err
	}

	return j, nil
}

// GetLoadJobResult is Load Job のResultsを取得する
// TODO Staticsとかの情報はLibが未対応っぽいので、stateがdoneになったかどうかだけ返している
func GetLoadJobResult(c context.Context, projectID string, jobID string) error {
	client, err := bq.NewClient(c, projectID)
	if err != nil {
		log.Errorf(c, "bigquery.NewClient err = %v", err)
		return err
	}

	job, err := client.JobFromID(c, jobID)
	if err != nil {
		log.Errorf(c, "bigquery.JobFromID err = %v", err)
		return err
	}

	js, err := job.Status(c)
	if err != nil {
		log.Errorf(c, "bigquery.Job.Status err = %v", err)
		return err
	}
	if js.Done() == false {
		return ErrJobWorking
	}

	if js.Err() != nil {
		log.Errorf(c, "bigquery.JobStatus err = %v", err)
		return js.Err()
	}

	return nil
}
