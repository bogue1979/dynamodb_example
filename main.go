package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func obsdata(svc *dynamodb.DynamoDB, table, bucket, from, to string) ([]ObservationPath, error) {

	var ret []ObservationPath

	params := &dynamodb.QueryInput{
		TableName: aws.String(table), // Required
		Limit:     aws.Int64(1000),
		IndexName: aws.String("datasetId-validAt-index"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v_ID": { // Required
				S: aws.String(bucket),
			},
			":v_Valid1": {
				S: aws.String(from),
			},
			":v_Valid2": {
				S: aws.String(to),
			},
		},
		//FilterExpression: aws.String("age >= :v_age"),

		KeyConditionExpression: aws.String("datasetId = :v_ID AND validAt BETWEEN :v_Valid1 AND :v_Valid2"),
		Select:                 aws.String("ALL_ATTRIBUTES"),
		ScanIndexForward:       aws.Bool(true),
	}

	//Get the response
	resp, err := svc.Query(params)

	if err != nil {
		log.Println("Query Error: ", err.Error())
	}
	//log.Println(awsutil.StringValue(resp))

	for i := 0; i < len(resp.Items); i++ {
		var bla ObservationPath
		err = dynamodbattribute.UnmarshalMap(resp.Items[i], &bla)
		if err != nil {
			return ret, fmt.Errorf("Oops: %s", err)
		}
		ret = append(ret, bla)
	}
	return ret, nil
}

// auth wrapper based on check function
func auth(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if !check(user, pass) {
			http.Error(w, "Unauthorized.", 401)
			return
		}
		fn(w, r)
	}
}

func check(user, pass string) bool {
	//fmt.Println("in check fn: ", user, " ", pass)
	users := map[string]string{
		"user":  "pass",
		"user2": "pass2",
	}
	if user == "" || pass == "" {
		return false
	}
	if users[user] == pass {
		return true
	}
	return false
}

// example: curl  -u user:pass "localhost:8080/radar/observation/obs-radar.ukmo.uk.sh.s3.mg%2F1km%2Fintensity?timestampFrom=2016-09-08T11%3A58%3A13Z&timestampTo=2016-09-08T12%3A58%3A13Z"
func observationsHandler(w http.ResponseWriter, r *http.Request) {
	table := "interactivemap-radardata-processing-prod-tileset-v2"
	from := r.FormValue("timestampFrom")
	to := r.FormValue("timestampTo")
	bucket := r.URL.Path[19:]

	// TODO: validate input
	//       response code if error ...

	//bucket := "fcst-radar.mg.uk.sh.s3.mg/1km/precipitationtype"
	//from := "2016-09-01T11:25:00Z"
	//to := "2016-09-08T11:25:00Z"
	obs, err := obsdata(svc, table, bucket, from, to)
	if err != nil {
		// TODO: set response error code
		fmt.Fprintf(w, "error: %s", err)
	}

	//fmt.Fprintf(w, "Table: %s  From: %s To: %s", bucket, from, to)

	/// construct response json
	var out ObservationOut

	out.BoundingBox = obs[0].BoundingBox
	out.MinZoomLevel = obs[0].Tileset.MinZoom
	out.MaxZoomLevel = obs[0].Tileset.MaxZoom

	for i := 0; i < len(obs); i++ {
		timestamp := obs[i].IngestedAt // seems to be based on tilesurl

		data := struct {
			TileURL   string `json:"tileUrl"`
			TimeStamp string `json:"_timestamp"`
		}{obs[i].Tileset.TilesURITemplate, timestamp}

		out.TimeSeries = append(out.TimeSeries, data)
	}

	o, err := json.Marshal(out)
	if err != nil {
		fmt.Println("OOps error: ", err)
	}

	outstring := string(o)

	//w.Header().Set("Content-Type", "application/vnd.mg.timeseries+json;charset=UTF-8"  //WTF ???
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("Content-Length", strconv.Itoa(len(outstring)))

	fmt.Fprintf(w, "%s", outstring)
}

var svc = dynamodb.New(session.New(&aws.Config{Region: aws.String("eu-west-1")}))

func main() {

	http.HandleFunc("/radar/observation/", auth(observationsHandler))
	http.ListenAndServe(":8080", nil)
}

// ObservationOut is constructor for output
type ObservationOut struct {
	BoundingBox struct {
		Northeast struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"northeast"`
		Southwest struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"southwest"`
	} `json:"boundingbox"`
	MinZoomLevel int64 `json:"minZoomLevel"`
	MaxZoomLevel int64 `json:"maxZoomLevel"`
	TimeSeries   []struct {
		TileURL   string `json:"tileUrl"`
		TimeStamp string `json:"_timestamp"`
	}
}

// ObservationPath is response struct for /radar/observation
type ObservationPath struct {
	DatasetID  string
	IngestedAt string
	RunAt      string
	TilesetID  string
	Tileset    struct {
		Height           int64
		Width            int64
		MaxZoom          int64
		MinZoom          int64
		TilesURITemplate string
	}
	BoundingBox struct {
		Northeast struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"northeast"`
		Southwest struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"southwest"`
	} `json:"boundingbox"`
}
