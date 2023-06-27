package hopper

import (
	"log"
	"testing"
)

func TestDelete(t *testing.T) {
	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")
	id, err := db.Insert("users", Map{"name": "foo"})
	if err != nil {
		t.Fatal(err)
	}
	delete := Map{"id": id}
	if err := db.Delete("users").Eq(delete).Exec(); err != nil {
		t.Fatal(err)
	}
	records, err := db.Find("users").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("expected to have 0 records got %d", len(records))
	}
}

func TestUpdate(t *testing.T) {
	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")
	_, err = db.Insert("users", Map{"name": "foo"})
	if err != nil {
		t.Fatal(err)
	}
	values := Map{"name": "bar"}
	results, err := db.Update("users").Values(values).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		log.Fatalf("expected to have 1 result got %d", len(results))
	}
	records, err := db.Find("users").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		log.Fatalf("expected to have 1 result got %d", len(results))
	}
	if records[0]["name"] != values["name"] {
		t.Fatalf("expected name to be %s got %s", values["name"], records[0]["name"])
	}
}

func TestInsert(t *testing.T) {
	values := []Map{
		{
			"name": "Foo",
			"age":  10,
		},
		{
			"name": "Bar",
			"age":  88.3,
		},
		{
			"name": "Baz",
			"age":  10,
		},
	}

	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")
	for i, data := range values {
		id, err := db.Insert("users", data)
		if err != nil {
			t.Fatal(err)
		}
		if id != uint64(i+1) {
			t.Fatalf("expect ID %d got %d", i, id)
		}
	}
	users, err := db.Find("users").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != len(values) {
		t.Fatalf("expecting %d result got %d", len(values), len(users))
	}
}

func TestFind(t *testing.T) {
	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")

	coll := "users"
	_, err = db.Insert(coll, Map{"username": "James007"})
	if err != nil {
		t.Fatal(err)
	}

	results, err := db.Find("users").Eq(Map{}).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result got %d", len(results))
	}
}
