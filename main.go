package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"

	"github.com/storkgrass/opclogger/config"
)
type EnvVars struct {
	DATABASE_URL          string
	OPCUA_ENDPOINT        string
	OPCUA_SECURITY_POLICY string
	OPCUA_SECURITY_MODE   string
	TIME_COLUMN_NAME      string
	OPCUA_MAXRETRIES      int
	OPCUA_TIMEOUT         int
	DATABASE_MAXRETRIES   int
	DATABASE_TIMEOUT      int
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to loading .env file: %v", err)
	}

	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("failed to load the config.json: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Listen for OS interrupt or termination signals and trigger graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		// Block until a signal is received
		sig := <-sigs
		log.Printf("received OS signal: %s. Initiating graceful shutdown...", sig)
		cancel()
	}()

	// Establish a connection to the PostgreSQL database
	dbURL := os.Getenv("DATABASE_URL")
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	// Establish a connection to the OPC UA server
	opcURL := os.Getenv("OPCUA_ENDPOINT")
	opcSecurityPolicy := os.Getenv("OPCUA_SECURITY_POLICY")
	opcSecurityMode := os.Getenv("OPCUA_SECURITY_MODE")
	client, err := newOPCUAClient(opcURL, opcSecurityPolicy, opcSecurityMode, ctx)
	if err != nil {
		log.Fatalf("failed to initialize OPC UA client: %v", err)
	}
	defer client.Close(ctx)

	// Grouping TagGroups by their interval time
	intervalMap := make(map[int][]config.TagGroup)
	for _, gp := range cfg.TagGroups {
		intervalMap[gp.Interval] = append(intervalMap[gp.Interval], gp)
	}

	// Main Loop
	for interval, tables := range intervalMap {
		wg.Add(1)
		go func(interval int, groups []config.TagGroup, ctx context.Context) {
			defer wg.Done()
			ticker := time.NewTicker(time.Duration(interval) * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					for _, gp := range groups {
						err := readValues(gp.Tags, client, ctx)
						if err != nil {
							//TODO
							continue
						}
						err = writeDatabase(gp, db, ctx)
						if err != nil {
							//TOOD
							continue
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}(interval, tables, ctx)
	}

func getEnvOrDefault(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}

func loadEnvironment() (*EnvVars, error) {
	if err := godotenv.Load(".env"); err != nil {
		logger.Warningf(".env file not found or failed to load: %v. Continuing with existing environment variables.", err)
	}

	appEnv := getEnvOrDefault("APP_ENV", "development")
	pattern := fmt.Sprintf(".env.%s*", appEnv)
	if matches, err := filepath.Glob(pattern); err == nil && len(matches) > 0 {
		for _, match := range matches {
			if err := godotenv.Overload(match); err != nil {
				logger.Warningf("Failed to load environment file %s: %v", match, err)
			}
		}
	}

	requiredVars := []string{
		"DATABASE_URL",
		"OPCUA_ENDPOINT",
		"OPCUA_SECURITY_POLICY",
		"OPCUA_SECURITY_MODE",
	}
	for _, varName := range requiredVars {
		_, ok := os.LookupEnv(varName)
		if !ok {
			return nil, fmt.Errorf("environment variable %s is not set. Please check your .env file or system environment variables.", varName)
		}
	}

	envVars := &EnvVars{
		DATABASE_URL:          os.Getenv("DATABASE_URL"),
		OPCUA_ENDPOINT:        os.Getenv("OPCUA_ENDPOINT"),
		OPCUA_SECURITY_POLICY: os.Getenv("OPCUA_SECURITY_POLICY"),
		OPCUA_SECURITY_MODE:   os.Getenv("OPCUA_SECURITY_MODE"),
		TIME_COLUMN_NAME:      getEnvOrDefault("TIME_COLUMN_NAME", "timestamp"),
	}

	var err error
	if envVars.OPCUA_MAXRETRIES, err = strconv.Atoi(getEnvOrDefault("OPCUA_MAXRETRIES", "1000000")); err != nil || envVars.OPCUA_MAXRETRIES < 1 {
		return nil, fmt.Errorf("OPCUA_MAXRETRIES must be a positive integer: %v", err)
	}
	if envVars.OPCUA_TIMEOUT, err = strconv.Atoi(getEnvOrDefault("OPCUA_TIMEOUT", "5")); err != nil || envVars.OPCUA_TIMEOUT < 1 {
		return nil, fmt.Errorf("OPCUA_TIMEOUT must be a positive integer: %v", err)
	}
	if envVars.DATABASE_MAXRETRIES, err = strconv.Atoi(getEnvOrDefault("DATABASE_MAXRETRIES", "1000000")); err != nil || envVars.DATABASE_MAXRETRIES < 1 {
		return nil, fmt.Errorf("DATABASE_MAXRETRIES must be a positive integer: %v", err)
	}
	if envVars.DATABASE_TIMEOUT, err = strconv.Atoi(getEnvOrDefault("DATABASE_TIMEOUT", "5")); err != nil || envVars.DATABASE_TIMEOUT < 1 {
		return nil, fmt.Errorf("DATABASE_TIMEOUT must be a positive integer: %v", err)
	}

	return envVars, nil
}
}

func newOPCUAClient(endpointURL string, securityPolicy string, securityMode string, ctx context.Context) (*opcua.Client, error) {
	opcOpts := []opcua.Option{
		opcua.SecurityPolicy(securityPolicy),
		opcua.SecurityModeString(securityMode),
	}
	client, err := opcua.NewClient(endpointURL, opcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create instance of the OPC UA client: %v", err)
	}
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to the OPC UA server: %v", err)
	}

	return client, nil
}

func readValues(tags []config.Tag, client *opcua.Client, ctx context.Context) error {
	nodeIDs := make([]*ua.ReadValueID, 0, len(tags))
	for i := range tags {
		nodeIDs = append(nodeIDs, &ua.ReadValueID{
			NodeID: tags[i].NodeID,
			// AttributeID: ua.AttributeIDValue,
		})
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req := &ua.ReadRequest{
		MaxAge:             0,
		NodesToRead:        nodeIDs,
		TimestampsToReturn: ua.TimestampsToReturnBoth,
	}

	resp, err := client.Read(timeoutCtx, req)
	if err != nil {
		return err
	}

	if resp.Results == nil || len(resp.Results) != len(tags) {
		return fmt.Errorf("invalid response: %v", resp.Results)
	}

	for i := range tags {
		result := resp.Results[i]
		if result.Status != ua.StatusOK {
			continue
		}
		tags[i].Value = result.Value.Value()
	}

	return nil
}

func writeDatabase(group config.TagGroup, db *sqlx.DB, ctx context.Context) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if len(group.Tags) == 0 {
		return nil
	}

	columns := []string{"time"}
	values := []any{time.Now()}
	for _, tag := range group.Tags {
		if tag.Value != nil {
			columns = append(columns, tag.ColumnName)
			values = append(values, tag.Value)
		}
	}

	if len(columns) == 1 {
		return nil
	}

	query, args, err := sqlx.In(fmt.Sprintf("INSERT INTO %s (%s) VALUES (?);", group.TableName, strings.Join(columns, ",")), values)
	if err != nil {
		return fmt.Errorf("failed to construct SQL query: %v", err)
	}
	query = db.Rebind(query)

	tx, err := db.BeginTxx(timeoutCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to execution of the query: %v", err)
	}

	return tx.Commit()
}
