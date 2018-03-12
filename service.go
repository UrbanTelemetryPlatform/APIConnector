package main

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"google.golang.org/appengine"
)

type output struct {
	success bool
	error   string
	data    interface{}
}

func main() {
	router := mux.NewRouter()

	//Register api paths
	router.Path("/api/read").Methods("GET").HandlerFunc(readAPI)
	router.Path("/api/auth").Methods("GET").HandlerFunc(readAPI)

	//Startup
	http.Handle("/", router)
	fmt.Println("server is starting up now")
	appengine.Main()
}

func readAPI(w http.ResponseWriter, r *http.Request) {

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
