package main

import (
	"net/http"
	"fmt"
	"log"
	"time"
)

func mainHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
	w.WriteHeader(200)
}

func main() {

	ticker := time.NewTicker(5 * time.Minute)

	http.HandleFunc("/main", mainHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
