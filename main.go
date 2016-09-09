package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func obsdata(svc *dynamodb.DynamoDB, table, bucket, from, to string) ([]ObservationPath, error) {

	var ret []ObservationPath

	params := &dynamodb.QueryInput{
		TableName: aws.String(table), // Required
		Limit:     aws.Int64(3),
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

// example: curl "localhost:8080/radar/observation/obs-radar.ukmo.uk.sh.s3.mg%2F1km%2Fintensity?timestampFrom=2016-09-08T11%3A58%3A13Z&timestampTo=2016-09-08T12%3A58%3A13Z"

func observations(w http.ResponseWriter, r *http.Request) {
	table := "interactivemap-radardata-processing-prod-tileset-v2"
	from := r.FormValue("timestampFrom")
	to := r.FormValue("timestampTo")
	bucket := r.URL.Path[19:]

	// TODO: validate input
	//       set header
	//       response code if error ...

	//bucket := "fcst-radar.mg.uk.sh.s3.mg/1km/precipitationtype"
	//from := "2016-09-01T11:25:00Z"
	//to := "2016-09-08T11:25:00Z"

	obs, err := obsdata(svc, table, bucket, from, to)
	if err != nil {
		// TODO: set response error code
		fmt.Fprintf(w, "error: %s", err)
	}

	//TODO: json encode into final object
	fmt.Fprintf(w, "Table: %s  From: %s To: %s", bucket, from, to)

	fmt.Fprintf(w, "%#v", obs)

}

var svc = dynamodb.New(session.New(&aws.Config{Region: aws.String("eu-west-1")}))

func main() {

	http.HandleFunc("/radar/observation/", observations)
	http.ListenAndServe(":8080", nil)

	//fmt.Printf("%v\n", obs)

	//fmt.Printf("%#v", resp.Items[0]["boundingBox"])

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
			Latitude  float64
			Longitude float64
		}
		Southwest struct {
			Latitude  float64
			Longitude float64
		}
	}
}
