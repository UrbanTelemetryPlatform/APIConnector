package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
	sjson "github.com/bitly/go-simplejson"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

type output struct {
	success bool
	error   string
	data    interface{}
}

const tsAPILayout string = "2006-01-02 15:04:05.0"
const tsMessageFormat string = "2006-01-02 15:04:05"

func main() {
	http.HandleFunc("/api/auth", authCallback)
	http.HandleFunc("/api/read", readAPI)
	http.HandleFunc("/api/welcome", sayHello)
	appengine.Main()
}

func sayHello(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "This is the APIConnector \n")
}

func readAPI(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	log.Infof(ctx, "New API Data Read Started")

	token := "MUPHbw0OutH5Hnp0UT6lgM5rc"
	apiurl := "http://data.cityofchicago.org/resource/8v9j-bter.json"
	projectID := "utp-md"
	topicName := "traffic_update"

	client := getHTTPClient(ctx)

	//Execute GET
	resp, err := client.Get(apiurl + "?$$app_token=" + token + "&$limit=1400")
	if err != nil {
		log.Errorf(ctx, "Error fetching URL"+err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Get body
	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf(ctx, "Error getting body "+err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//Convert JSON
	jsonArray, err := sjson.NewJson([]byte(response))
	if err != nil {
		log.Errorf(ctx, "Error to convert json"+err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	realArray, _ := jsonArray.Array()
	len := len(realArray)
	fmt.Fprintf(w, "Response from API %v\n", len)

	// Creates a pubsub client.
	psClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Errorf(ctx, "Error to create pubsub client "+err.Error())
		http.Error(w, "Failed to create pubsub client "+err.Error(), http.StatusInternalServerError)
		return
	}

	messageCnt := 0

	topic := psClient.Topic(topicName)
	for i := 0; i < len; i++ {

		entry := jsonArray.GetIndex(i)

		message := sjson.New()

		segmentID, _ := entry.Get("segmentid").Int()
		speed, _ := entry.Get("_traffic").Int()
		timeStr, err := entry.Get("_last_updt").String()
		timestamp, err := time.Parse(tsAPILayout, timeStr)

		if err != nil {
			log.Errorf(ctx, "Error handling json "+err.Error())
			continue
		}

		if timestamp.Year() != 2018 {
			continue
		}

		message.Set("TIME", timestamp.Format(tsMessageFormat)+" -05:00")

		message.Set("SEGMENTID", segmentID)
		message.Set("BUS_COUNT", 1)
		message.Set("MESSAGE_COUNT", 1)
		message.Set("SPEED", speed)

		messageData, err := message.MarshalJSON()
		if err != nil {
			continue
		}

		result := topic.Publish(ctx, &pubsub.Message{
			Data: messageData,
		})

		messageID, err := result.Get(ctx)
		if err == nil {
			messageCnt++
		}

		_ = messageID
	}

	fmt.Fprintf(w, "Published %v messages successfully\n", messageCnt)
}

func authCallback(w http.ResponseWriter, r *http.Request) {

}

func getHTTPClient(ctx context.Context) *http.Client {
	return &http.Client{
		Transport: &urlfetch.Transport{
			Context: ctx,
			AllowInvalidServerCertificate: true,
		},
	}
}

func checkErrors(w http.ResponseWriter, err error) {
	if err != nil {
		fmt.Fprint(w, err.Error())
		w.WriteHeader(500)
		panic(err.Error())
	}
}
