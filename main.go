package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"github.com/kardianos/service"
	_ "github.com/lib/pq"

	"github.com/storkgrass/opclogger/config"
)

var logger service.Logger

type program struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

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

func (p *program) Start(s service.Service) error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	go func() {
		if err := p.run(); err != nil {
			logger.Errorf("Run error: %v", err)
			p.cancel()
		}
	}()

	return nil
}

func (p *program) Stop(s service.Service) error {
	// Any work in Stop should be quick, usually a few seconds at most.
	logger.Info("stopping...")
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Graceful shutdown complete.")
	case <-time.After(2 * time.Second):
		logger.Warning("Forced shutdown due to timeout.")
	}

	return nil
}

func main() {
	svcFlag := flag.String("service", "", "Control the system service.")
	flag.Parse()

	options := make(service.KeyValue)
	options["Restart"] = "on-failure"
	options["RestartSec"] = "3"
	options["SuccessExitStatus"] = "0 SIGTERM"
	options["StandardOutput"] = "journal"
	svcConfig := &service.Config{
		Name:        "opclogger-service",
		DisplayName: "OPC UA Data Logging Service",
		Description: "A service that collects data from an OPC UA server and logs it to a database.",
		Dependencies: []string{
			"Requires=network.target",
			"After=network-online.target syslog.target"},
		Option: options,
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	errs := make(chan error, 5)
	logger, err = s.Logger(errs)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			select {
			case err := <-errs:
				if err != nil {
					logger.Errorf("service error: %v", err)
				}
			case <-prg.ctx.Done():
				return
			}
		}
	}()

	if len(*svcFlag) != 0 {
		err := service.Control(s, *svcFlag)
		if err != nil {
			log.Printf("Failed to control service with action %s: %v", *svcFlag, err)
			log.Printf("Valid actions: %q", service.ControlAction)
			log.Fatal(err)
		}
		return
	}
	err = s.Run()
	if err != nil {
		logger.Error(err)
	}
}

func (p *program) run() error {
	envVars, err := loadEnvironment()
	if err != nil {
		return err
	}

	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		return fmt.Errorf("failed to load the config.json: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("failed to validate the config.json: %v", err)
	}

	// Establish a connection to the PostgreSQL database
	db, err := initDatabase(p.ctx, envVars.DATABASE_URL)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %v", err)
	}
	defer db.Close()

	// Establish a connection to the OPC UA server
	client, err := initOPCUAClient(p.ctx, envVars.OPCUA_ENDPOINT, envVars.OPCUA_SECURITY_POLICY, envVars.OPCUA_SECURITY_MODE)
	if err != nil {
		return fmt.Errorf("failed to initialize OPC UA client: %v, endpoint: %s, security policy: %s, security mode: %s", err, envVars.OPCUA_ENDPOINT, envVars.OPCUA_SECURITY_POLICY, envVars.OPCUA_SECURITY_MODE)
	}
	defer client.Close(context.Background())

	// Grouping TagGroups by their interval time
	intervalMap := make(map[int][]config.TagGroup, len(cfg.TagGroups))
	for _, gp := range cfg.TagGroups {
		intervalMap[gp.Interval] = append(intervalMap[gp.Interval], gp)
	}

	// Main Loop
	for interval, groups := range intervalMap {
		p.wg.Add(1)
		go func(interval int, groups []config.TagGroup, ctx context.Context) {
			defer p.wg.Done()
			ticker := time.NewTicker(time.Duration(interval) * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					for i := range groups {
						if err := readValues(ctx, groups[i].Tags, client); err != nil {
							if state := client.State(); state == opcua.Disconnected {
								logger.Warningf("OPC UA client is disconnected, retrying connection...")
								if err := retryOpcuaConnect(ctx, client, envVars.OPCUA_MAXRETRIES, time.Duration(envVars.OPCUA_TIMEOUT)*time.Second); err != nil {
									logger.Errorf("failed to reconnect to the OPC UA server: %v, endpoint: %s", err, envVars.OPCUA_ENDPOINT)
								}
							} else {
								logger.Errorf("failed to read values: %v, tag group: %s", err, groups[i].TableName)
							}
							continue
						}

						if err = writeDatabase(ctx, envVars.TIME_COLUMN_NAME, groups[i], db); err != nil {
							if err := retryDatabaseConnect(ctx, db, envVars.DATABASE_MAXRETRIES, time.Duration(envVars.DATABASE_TIMEOUT)*time.Second); err != nil {
								logger.Errorf("failed to reconnect to the database: %v", err)
							}
							logger.Errorf("failed to write to database: %v", err)
							continue
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}(interval, groups, p.ctx)
	}

	return nil
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

func initDatabase(ctx context.Context, dbURL string) (*sqlx.DB, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping the database: %v", err)
	}
	return db, nil
}

func initOPCUAClient(ctx context.Context, endpointURL string, securityPolicy string, securityMode string) (*opcua.Client, error) {
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

func retryOpcuaConnect(ctx context.Context, client *opcua.Client, maxRetries int, timeout time.Duration) error {
	return retryWithExponentialBackoff(ctx, "connect to OPC UA server", maxRetries, func(ctx context.Context) error {
		return client.Connect(ctx)
	}, timeout)
}

func retryDatabaseConnect(ctx context.Context, db *sqlx.DB, maxRetries int, timeout time.Duration) error {
	return retryWithExponentialBackoff(ctx, "connect to database", maxRetries, func(ctx context.Context) error {
		return db.PingContext(ctx)
	}, timeout)
}

func retryWithExponentialBackoff(ctx context.Context, operation string, maxRetries int, fn func(context.Context) error, timeout time.Duration) error {
	if maxRetries < 1 {
		return fmt.Errorf("maxRetries must be greater than 0")
	}
	if timeout < 1 {
		return fmt.Errorf("timeout must be greater than 0")
	}

	var err error
	for retry := range maxRetries {
		logger.Infof("[%d/%d] Retrying operation: %s", retry+1, maxRetries, operation)
		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
		err = fn(timeoutCtx)
		cancel()
		if err == nil {
			return nil
		}

		// Simple exponential backoff strategy: 2^retry seconds, up to 60 seconds
		backoffDuration := time.Duration(math.Min(math.Pow(2, float64(retry)), 60)) * time.Second

		select {
		case <-time.After(backoffDuration):
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("failed to %s after %d retries: %v", operation, maxRetries, err)
}

func readValues(ctx context.Context, tags []config.Tag, client *opcua.Client) error {
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

	if resp.Results == nil {
		return fmt.Errorf("received nil results from OPC UA read operation for tags: %v", tags)
	}

	if len(resp.Results) != len(tags) {
		return fmt.Errorf("expected %d results but got %d for tags: %v", len(tags), len(resp.Results), tags)
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

func writeDatabase(ctx context.Context, timeColumn string, group config.TagGroup, db *sqlx.DB) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if len(group.Tags) == 0 {
		return nil
	}

	columns := []string{timeColumn}
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
