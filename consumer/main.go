package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		// Your code to publish a message goes here.
		fmt.Fprintf(w, "Message published!")
	})

	http.ListenAndServe(":8080", nil)
}
