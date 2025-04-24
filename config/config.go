package config

import (
	"encoding/json"
	"log"
	"os"

	"github.com/gopcua/opcua/ua"
)

type Config struct {
	TagGroups []TagGroup `json:"tag_groups"`
}

type TagGroup struct {
	TableName string `json:"table_name"`
	Interval  int    `json:"interval"`
	Tags      []Tag  `json:"tags"`
}

type Tag struct {
	ID         string     `json:"id"`
	NodeID     *ua.NodeID `json:"-"`
	ColumnName string     `json:"column_name"`
	Value      any        `json:"-"`
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config file %s: %v", path, err)
	}

	for _, gp := range cfg.TagGroups {
		for i := range gp.Tags {
			nodeID, err := ua.ParseNodeID(gp.Tags[i].ID)
			if err != nil {
				log.Printf("parseNodeID failed: %v", err)
				continue
			}
			gp.Tags[i].NodeID = nodeID
		}
	}

	return &cfg, nil
}
