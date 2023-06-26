package main

import (
	"fmt"
	"log"

	"github.com/anthm/hopper/hopper"
)

func main() {
	db, err := hopper.New()
	if err != nil {
		log.Fatal(err)
	}
	user := map[string]string{
		"name": "Anthony",
		"age":  "36",
	}
	id, err := db.Insert("users", user)
	if err != nil {
		log.Fatal(err)
	}
	user["name"] = "james"
	id, err = db.Insert("users", user)
	if err != nil {
		log.Fatal(err)
	}

	_ = id
	results, err := db.Find("users", hopper.Filter{})
	if err != nil {
		log.Fatal(err)
	}

	// coll, err := db.CreateCollection("users")
	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Printf("%+v\n", results)
}
