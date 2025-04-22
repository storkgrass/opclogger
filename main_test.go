package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/gopcua/opcua/ua"
	"github.com/joho/godotenv"
	"github.com/storkgrass/opclogger/config"
)

var tags = []config.Tag{
	{
		ID:         "ns=4;s=|var|CODESYS Control for Raspberry Pi MC SL.Application.GVL.bV001",
		ColumnName: "bV001",
	},
	{
		ID:         "ns=4;s=|var|CODESYS Control for Raspberry Pi MC SL.Application.GVL.iV002",
		ColumnName: "iV002",
	},
	{
		ID:         "ns=4;s=|var|CODESYS Control for Raspberry Pi MC SL.Application.GVL.rV003",
		ColumnName: "rV003",
	},
}

func TestMain(m *testing.M) {
	err := godotenv.Load(".env.test")
	if err != nil {
		fmt.Printf("failed to load test .env file: %v", err)
		os.Exit(1)
	}

	for i := range tags {
		nodeID, err := ua.ParseNodeID(tags[i].ID)
		if err != nil {
			fmt.Printf("failed to parse nodeID: %v", nodeID)
			os.Exit(1)
		}
		tags[i].NodeID = nodeID
	}

	code := m.Run()
	os.Exit(code)
}

func TestReadValues(t *testing.T) {
	ctx := t.Context()

	opcURL := os.Getenv("OPCUA_ENDPOINT")
	opcSecurityPolicy := os.Getenv("OPCUA_SECURITY_POLICY")
	opcSecurityMode := os.Getenv("OPCUA_SECURITY_MODE")

	c, err := newOPCUAClient(opcURL, opcSecurityPolicy, opcSecurityMode, ctx)
	if err != nil {
		t.Fatalf("failed to create instance of the OPC UA client: %v", err)
	}
	defer c.Close(ctx)

	t.Logf("Connecting to OPC UA server at: %s", opcURL)

	err = readValues(tags, c, ctx)
	if err != nil {
		t.Errorf("failed to readValues: %v", err)
	}

	for i := range tags {
		t.Logf("%s = %v", tags[i].ID, tags[i].Value)
	}
}
