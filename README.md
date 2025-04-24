# opclogger

`opclogger` is a service that collects data from an OPC UA server and logs it into a PostgreSQL database. This program is designed to run as a service on both **Windows** and **Linux (systemd)**.

## Requirements

- **Go**: Version 1.18 or higher
- **Dependencies**:
  - `github.com/gopcua/opcua`
  - `github.com/jmoiron/sqlx`
  - `github.com/kardianos/service`
  - `github.com/joho/godotenv`


## Environment Variables

The following environment variables should be set in a `.env` file or as system environment variables:

| Variable Name           | Description                               | Default Value |
| ----------------------- | ----------------------------------------- | ------------- |
| `DATABASE_URL`          | PostgreSQL connection URL                 | -             |
| `OPCUA_ENDPOINT`        | OPC UA server endpoint URL                | -             |
| `OPCUA_SECURITY_POLICY` | OPC UA security policy                    | -             |
| `OPCUA_SECURITY_MODE`   | OPC UA security mode                      | -             |
| `TIME_COLUMN_NAME`      | Timestamp column name in the database     | `timestamp`   |
| `OPCUA_MAXRETRIES`      | Maximum retries for OPC UA connection     | `1000000`     |
| `OPCUA_TIMEOUT`         | Timeout for OPC UA connection (seconds)   | `5`           |
| `DATABASE_MAXRETRIES`   | Maximum retries for database connection   | `1000000`     |
| `DATABASE_TIMEOUT`      | Timeout for database connection (seconds) | `5`           |

---

## Environment Configuration

This program loads environment variables from a `.env` file located in the same directory as the binary by default.


## Installing as a Windows Service

1. Build the binary 

   Build the program to create an executable file:

    ```bash
    go build -o opclogger.exe
    ```
   
2. Install the service
   
    ```bash
    opclogger.exe --service install
    ```

3. Start the service

    ```bash
    opclogger.exe --service start
    ```

4. Stop the service

    ```bash
    opclogger.exe --service stop
    ```

5. Stop the service

    ```bash
    opclogger.exe --service restart
    ```

6. Uninstall the service
   
    ```bash
    opclogger.exe --service uninstall
    ```

## Installing as a Linux (Systemd) Service

1. Build the binary

    Build the program and place the binary in `/usr/local/bin`:

    ```
    go build -o /usr/local/bin/opclogger
    ```

2. Install the service

    ```bash
    /usr/local/bin/opclogger --service install
    ```

   Check the generated service file:
    ```
    cat /etc/systemd/system/opclogger-service.service
    ```

    Modify it as needed. Example:
    ```ini
    [Unit]
    Description=OPC UA Data Logging Service
    Requires=network.target
    After=network.target syslog.target

    [Service]
    ExecStart=/usr/local/bin/opclogger
    Restart=on-failure
    User=your-username
    Group=your-groupname
    EnvironmentFile=/etc/default/opclogger.env
    WorkingDirectory=/usr/local/bin

    [Install]
    WantedBy=multi-user.target
    ```

3. Enable and start the service

    ```
    sudo systemctl enable opclogger-service
    sudo systemctl start opclogger-service
    ```
