package kafka

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

var registry *Registry

//Registry provides an interface to interact with Confluent schema registry
type Registry struct {
	registryURL string
	client      *http.Client
	schemaMap   sync.Map
}

//NewRegistry ....
func NewRegistry(registryURL string) {
	registry = &Registry{
		registryURL: registryURL,
		client:      &http.Client{Timeout: 20 * time.Second},
		schemaMap:   sync.Map{},
	}
}

func (reg *Registry) parseAndCache(content []byte, id string) error {
	var msg map[string]interface{}
	err := json.Unmarshal(content, &msg)
	if err != nil {
		return err
	}

	schema, ok := msg["schema"].(string)
	registry.schemaMap.Store(id, schema)
	if !ok {
		return errors.New("Schema object missing in registry content for schema id:" + id)
	}

	return nil
}

//GetSchemaByID ...
func GetSchemaByID(id string) (string, error) {
	if registry == nil {
		return "", errors.New("kafka schema registry not configured, cannot fetch scheme from remote server")
	}
	if schema, ok := registry.schemaMap.Load(id); ok {
		return schema.(string), nil
	}
	resp, err := registry.client.Get(registry.registryURL + "/schemas/ids/" + id)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	err = registry.parseAndCache(content, id)
	if err != nil {
		return "", err
	}

	schema, ok := registry.schemaMap.Load(id)
	if !ok {
		return "", errors.New("Can't load schema for schema id:" + id)
	}

	return schema.(string), nil
}
