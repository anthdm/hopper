package hopper

import (
	"testing"
)

func TestUpdate(t *testing.T) {
	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")
	data := Map{"name": "foobarbaz"}
	_, err = db.Insert("users", data)
	if err != nil {
		t.Fatal(err)
	}
	update := Map{"name": "Sailor"}
	_, err = db.Update("users", Filter{}, update)
	if err != nil {
		t.Fatal(err)
	}

	results, err := db.Find("users", Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected to have 1 record got %d", len(results))
	}
	if results[0]["name"] != update["name"] {
		t.Fatalf("expected to have updated name to %s but got %s", update["name"], results[0]["name"])
	}
}

func TestSelect(t *testing.T) {
	db, err := New(WithDBName("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.DropDatabase("test")

	data := Map{"name": "foobarbaz"}
	_, err = db.Insert("users", data)
	if err != nil {
		t.Fatal(err)
	}
	filter := Filter{
		Select: []string{"name"},
	}
	results, err := db.Find("users", filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected to have 1 result got %d", len(results))
	}
	if _, ok := results[0]["id"]; ok {
		t.Fatal("didnt selected id")
	}
	if results[0]["name"] != data["name"] {
		t.Fatalf("expected name to be %s got %s", data["name"], results[0]["name"])
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
	users, err := db.Find("users", Filter{})
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

	data := Map{
		"name":    "Foobarbar",
		"isAdmin": true,
	}
	id, err := db.Insert("auth", data)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("expecting id 1 got %d", id)
	}
	results, err := db.Find("auth", Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expecting 1 result got %d", len(results))
	}
	result := results[0]
	if result["name"] != data["name"] {
		t.Fatalf("expected %s got %s", data["name"], result["name"])
	}
	if result["isAdmin"] != data["isAdmin"] {
		t.Fatalf("expected %b got %b", data["isAdmin"], result["isAdmin"])
	}
}
