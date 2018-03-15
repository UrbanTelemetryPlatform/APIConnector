package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"cloud.google.com/go/pubsub"
	"google.golang.org/appengine"
	"google.golang.org/appengine/urlfetch"
)

type output struct {
	success bool
	error   string
	data    interface{}
}

func main() {
	http.HandleFunc("/api/auth", authCallback)
	http.HandleFunc("/api/read", readAPI)
	appengine.Main()
}

func readAPI(w http.ResponseWriter, r *http.Request) {

	token := "MUPHbw0OutH5Hnp0UT6lgM5rc"
	apiurl := "https://data.cityofchicago.org/resource/8v9j-bter.json"
	projectID := "utp-md"
	topicName := "traffic_update"

	ctx := appengine.NewContext(r)
	client := urlfetch.Client(ctx)

	//Execute GET
	resp, err := client.Get(apiurl + "?$$app_token=" + token + "&$limit=1400")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Get body
	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var dataInResponse []interface{}
	err = json.Unmarshal([]byte(response), &dataInResponse)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "Response from API with %v entries\n", len(dataInResponse))

	// Creates a pubsub client.
	psClient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		http.Error(w, "Failed to create pubsub client "+err.Error(), http.StatusInternalServerError)
		return
	}

	messageCnt := 0

	topic := psClient.Topic(topicName)
	for i := 0; i < len(dataInResponse); i++ {
		messageData, _ := json.Marshal(dataInResponse[i])
		result := topic.Publish(ctx, &pubsub.Message{
			Data: messageData,
		})

		_, err := result.Get(ctx)
		if err == nil {
			messageCnt++
		}
	}

	fmt.Fprintf(w, "Published %v messages successfully\n", messageCnt)
}

func authCallback(w http.ResponseWriter, r *http.Request) {

}

func checkErrors(w http.ResponseWriter, err error) {
	if err != nil {
		fmt.Fprint(w, err.Error())
		w.WriteHeader(500)
		panic(err.Error())
	}
}
