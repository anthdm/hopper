package api

import (
	"encoding/json"
	"net/http"

	"github.com/anthm/hopper/hopper"
	"github.com/labstack/echo/v4"
)

type Server struct {
	db *hopper.Hopper
}

func NewServer(db *hopper.Hopper) *Server {
	return &Server{
		db: db,
	}
}

func (s *Server) HandlePostInsert(c echo.Context) error {
	var (
		collname = c.Param("collname")
	)
	var data hopper.Map
	if err := json.NewDecoder(c.Request().Body).Decode(&data); err != nil {
		return err
	}
	id, err := s.db.Insert(collname, data)
	if err != nil {
		return err
	}
	return c.JSON(http.StatusCreated, hopper.Map{"id": id})
}

func (s *Server) HandleGetQuery(c echo.Context) error {
	collname := c.Param("collname")
	records, err := s.db.Find(collname, hopper.Filter{})
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, records)
}
