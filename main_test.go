package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/storkgrass/opclogger/config"
)

var envVars = EnvVars{
	DATABASE_URL:          "postgres://user:password@localhost:5432/testdb",
	OPCUA_ENDPOINT:        "opc.tcp://localhost:4840",
	OPCUA_SECURITY_POLICY: "None",
	OPCUA_SECURITY_MODE:   "None",
	TIME_COLUMN_NAME:      "test_timestamp",
	OPCUA_MAXRETRIES:      3,
	OPCUA_TIMEOUT:         5,
	DATABASE_MAXRETRIES:   3,
	DATABASE_TIMEOUT:      5,
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestLoadEnvironment(t *testing.T) {
	envVars, err := loadEnvironment()
	if err != nil {
		t.Fatalf("loadEnvironment() failed: %v", err)
	}

	expected := EnvVars{
		DATABASE_URL:          "postgres://username:password@localhost:5432/opclog?sslmode=disable",
		OPCUA_ENDPOINT:        "opc.tcp://192.168.1.21:4840",
		OPCUA_SECURITY_POLICY: "None",
		OPCUA_SECURITY_MODE:   "None",
		TIME_COLUMN_NAME:      "timestamp",
		OPCUA_MAXRETRIES:      2,
		OPCUA_TIMEOUT:         1,
		DATABASE_MAXRETRIES:   3,
		DATABASE_TIMEOUT:      2,
	}

	if envVars.DATABASE_URL != expected.DATABASE_URL {
		t.Errorf("expected %s for DATABASE_URL, but got %s", expected.DATABASE_URL, envVars.DATABASE_URL)
	}
	if envVars.OPCUA_ENDPOINT != expected.OPCUA_ENDPOINT {
		t.Errorf("expected %s for OPCUA_ENDPOINT, but got %s", expected.OPCUA_ENDPOINT, envVars.OPCUA_ENDPOINT)
	}
	if envVars.OPCUA_SECURITY_POLICY != expected.OPCUA_SECURITY_POLICY {
		t.Errorf("expected %s for OPCUA_SECURITY_POLICY, but got %s", expected.OPCUA_SECURITY_POLICY, envVars.OPCUA_SECURITY_POLICY)
	}
	if envVars.OPCUA_SECURITY_MODE != expected.OPCUA_SECURITY_MODE {
		t.Errorf("expected %s for OPCUA_SECURITY_MODE, but got %s", expected.OPCUA_SECURITY_MODE, envVars.OPCUA_SECURITY_MODE)
	}
	if envVars.TIME_COLUMN_NAME != expected.TIME_COLUMN_NAME {
		t.Errorf("expected %s for TIME_COLUMN_NAME, but got %s", expected.TIME_COLUMN_NAME, envVars.TIME_COLUMN_NAME)
	}
	if envVars.OPCUA_MAXRETRIES != expected.OPCUA_MAXRETRIES {
		t.Errorf("expected %d for OPCUA_MAXRETRIES, but got %d", expected.OPCUA_MAXRETRIES, envVars.OPCUA_MAXRETRIES)
	}
	if envVars.OPCUA_TIMEOUT != expected.OPCUA_TIMEOUT {
		t.Errorf("expected %d for OPCUA_TIMEOUT, but got %d", expected.OPCUA_TIMEOUT, envVars.OPCUA_TIMEOUT)
	}
	if envVars.DATABASE_MAXRETRIES != expected.DATABASE_MAXRETRIES {
		t.Errorf("expected %d for DATABASE_MAXRETRIES, but got %d", expected.DATABASE_MAXRETRIES, envVars.DATABASE_MAXRETRIES)
	}
	if envVars.DATABASE_TIMEOUT != expected.DATABASE_TIMEOUT {
		t.Errorf("expected %d for DATABASE_TIMEOUT, but got %d", expected.DATABASE_TIMEOUT, envVars.DATABASE_TIMEOUT)
	}
}

func TestReadValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	c, err := initOPCUAClient(ctx, envVars.OPCUA_ENDPOINT, envVars.OPCUA_SECURITY_POLICY, envVars.OPCUA_SECURITY_MODE)
	if err != nil {
		t.Fatalf("failed to create instance of the OPC UA client: %v", err)
	}
	defer c.Close(ctx)

	tags := []config.Tag{
		{ID: "ns=4;s=|var|CODESYS Control for Raspberry Pi MC SL.Application.GVL.bV001"},
		{ID: "ns=4;s=|var|CODESYS Control for Raspberry Pi MC SL.Application.GVL.iV002"},
		{ID: "ns=4;s=|var|CODESYS Control for Raspberry Pi MC SL.Application.GVL.rV003"},
	}

	for i := range tags {
		nodeID, err := ua.ParseNodeID(tags[i].ID)
		if err != nil {
			fmt.Printf("failed to parse nodeID: %v", nodeID)
			os.Exit(1)
		}
		tags[i].NodeID = nodeID
	}

	err = readValues(ctx, tags, c)
	if err != nil {
		t.Errorf("failed to readValues: %v", err)
	}

	for i := range tags {
		t.Logf("%s = %v", tags[i].ID, tags[i].Value)
	}
}

func TestWriteDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	db, err := initDatabase(ctx, envVars.DATABASE_URL)
	if err != nil {
		t.Fatalf("failed to initialize the database connection: %v", err)
	}
	defer db.Close()

	defer func() {
		_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS test_table")
		if err != nil {
			t.Errorf("failed to drop test_table: %v", err)
		}
	}()

	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS test_table (test_timestamp TIMESTAMP, sw BOOLEAN, sensor1 INTEGER, sensor2 REAL)`)
	if err != nil {
		t.Fatalf("failed to create test_table: %v", err)
	}
	_, err = db.ExecContext(ctx, "DELETE FROM test_table")
	if err != nil {
		t.Fatalf("failed to delete from test_table: %v", err)
	}

	var group = config.TagGroup{
		TableName: "test_table",
		Interval:  1,
		Tags: []config.Tag{
			{
				ColumnName: "sw",
				Value:      bool(true),
			},
			{
				ColumnName: "sensor1",
				Value:      int(10),
			},
			{
				ColumnName: "sensor2",
				Value:      float32(0.75),
			},
		},
	}

	err = writeDatabase(ctx, envVars.TIME_COLUMN_NAME, group, db)
	if err != nil {
		t.Fatalf("failed to `writeDatabase`: %v", err)
	}

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM test_table")
	if err != nil {
		t.Fatalf("failed to query database: %v", err)
	}
	if count != 1 {
		t.Errorf("expected %d rows in database, but got %d", 1, count)
	}
}
