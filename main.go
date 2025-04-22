package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
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

	dbURL := os.Getenv("DATABASE_URL")
	// Establish a connection to the PostgreSQL database
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}
	defer db.Close()

	opcURL := os.Getenv("OPCUA_ENDPOINT")
	opcSecurityPolicy := os.Getenv("OPCUA_SECURITY_POLICY")
	opcSecurityMode := os.Getenv("OPCUA_SECURITY_MODE")
	// Establish a connection to the OPC UA server
	opcOpts := []opcua.Option{
		opcua.SecurityPolicy(opcSecurityPolicy),
		opcua.SecurityModeString(opcSecurityMode),
	}
	client, err := opcua.NewClient(opcURL, opcOpts...)
	if err != nil {
		log.Fatalf("failed to create instance of the OPC UA client: %v", err)
	}
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("failed to connect to the OPC UA server: %v", err)
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

	// Wait for the context to be done
	<-ctx.Done()

	// Create a new context with a timeout to wait for the WaitGroup
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()

	done := make(chan struct{})
	go func() {
		// Wait for all goroutines in the WaitGroup to complete
		wg.Wait()
		// Signal that the WaitGroup has finished
		close(done)
	}()

	select {
	case <-done:
		//TODO
		//Graceful shutdown
	case <-waitCtx.Done():
		//TODO
		//Forced shutdown due to timeout
	}
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
