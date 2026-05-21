# Setting up a Quack Server

This vignette covers how to set up and run a DuckDB Quack server for use
with datapond’s experimental Quack catalog backend.

**Status:** Quack is experimental (DuckDB 1.5.x). Production-ready
version planned for DuckDB 2.0 (Fall 2026).

------------------------------------------------------------------------

## What is Quack?

[Quack](https://duckdb.org/2026/05/12/quack-remote-protocol) is DuckDB’s
client-server protocol:

- **RPC protocol** for DuckDB-to-DuckDB communication
- **HTTP-based** for network compatibility (port 9494 by default)
- **Native serialisation** of DuckDB’s internal data vectors
- **Multi-writer support** without file locking

When used as a DuckLake catalog backend, Quack enables multiple
concurrent writers to the same catalog - something not possible with
DuckDB or SQLite file-based catalogs.

    Before Quack:                          After Quack:
    ┌─────────────────────────┐           ┌─────────────────────────┐
    │  Single Client Only     │           │  Multiple Clients       │
    │                         │           │                         │
    │  Client ──► DuckDB File │           │  Client A ──┐           │
    │             (locked)    │           │  Client B ──┼──► Quack  │
    │                         │           │  Client C ──┘   Server  │
    └─────────────────────────┘           └─────────────────────────┘

------------------------------------------------------------------------

## Basic Server Setup

### Development/Testing

Start a simple server for local development:

``` bash
duckdb -cmd "
  INSTALL quack;
  LOAD quack;
  INSTALL ducklake;
  LOAD ducklake;

  -- Create and attach the DuckLake catalog
  ATTACH 'ducklake:catalog.ducklake' AS lake (DATA_PATH '/data/lake');

  -- Start the Quack server
  SELECT * FROM quack_serve();
"
```

This prints connection details:

    ┌─────────────────────────────────┬────────────────────────────────┬──────────────────────────┐
    │ listen_uri                      │ http_url                       │ token                    │
    ├─────────────────────────────────┼────────────────────────────────┼──────────────────────────┤
    │ quack:localhost:9494            │ http://localhost:9494          │ abc123xyz...             │
    └─────────────────────────────────┴────────────────────────────────┴──────────────────────────┘

### Setting a Custom Token

For reproducible connections, set a known token:

``` bash
duckdb -cmd "
  INSTALL quack; LOAD quack;
  INSTALL ducklake; LOAD ducklake;

  -- Set a known token (min 4 characters)
  SET quack_token = 'my-secret-token';

  ATTACH 'ducklake:catalog.ducklake' AS lake (DATA_PATH '/data/lake');
  SELECT * FROM quack_serve();
"
```

### Connecting from R

``` r

library(datapond)

db_connect(
  catalog_type = "quack",
  metadata_path = "quack:localhost:9494/lake",
  data_path = "/data/lake",
  quack_token = "my-secret-token"
)
```

Or use the environment variable:

``` r

Sys.setenv(QUACK_TOKEN = "my-secret-token")

db_connect(
  catalog_type = "quack",
  metadata_path = "quack:localhost:9494/lake",
  data_path = "/data/lake"
)
```

------------------------------------------------------------------------

## Running as a Linux Service

For production, run the Quack server as a systemd service.

### 1. Create Startup Script

Create `/opt/duckdb/start-quack.sql`:

``` sql
INSTALL quack;
LOAD quack;
INSTALL ducklake;
LOAD ducklake;

-- Set authentication token from environment
SET quack_token = getenv('QUACK_TOKEN');

-- Attach the DuckLake catalog
ATTACH 'ducklake:/opt/duckdb/catalog.ducklake' AS lake
  (DATA_PATH '/data/lake');

-- Start server (blocks)
SELECT * FROM quack_serve(port := 9494);
```

### 2. Create systemd Service

Create `/etc/systemd/system/duckdb-quack.service`:

``` ini
[Unit]
Description=DuckDB Quack Server
After=network.target

[Service]
Type=simple
User=duckdb
Group=duckdb
Environment=QUACK_TOKEN=your-secret-token-here
WorkingDirectory=/opt/duckdb
ExecStart=/usr/local/bin/duckdb -init /opt/duckdb/start-quack.sql
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 3. Enable and Start

``` bash
# Create user
sudo useradd -r -s /bin/false duckdb
sudo mkdir -p /opt/duckdb /data/lake
sudo chown duckdb:duckdb /opt/duckdb /data/lake

# Enable service
sudo systemctl daemon-reload
sudo systemctl enable duckdb-quack
sudo systemctl start duckdb-quack

# Check status
sudo systemctl status duckdb-quack
journalctl -u duckdb-quack -f
```

------------------------------------------------------------------------

## Running with Docker

### Dockerfile

``` dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y wget unzip
RUN wget https://github.com/duckdb/duckdb/releases/download/v1.5.3/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin/

COPY start-quack.sql /opt/duckdb/
VOLUME ["/data"]
EXPOSE 9494

CMD ["duckdb", "-init", "/opt/duckdb/start-quack.sql"]
```

### Build and Run

``` bash
docker build -t duckdb-quack .
docker run -d \
  -p 9494:9494 \
  -v /path/to/data:/data \
  -e QUACK_TOKEN=my-secret-token \
  --name quack-server \
  duckdb-quack
```

### Docker Compose

``` yaml
version: '3.8'
services:
  quack:
    build: .
    ports:
      - "9494:9494"
    volumes:
      - ./data:/data
      - ./catalog:/opt/duckdb
    environment:
      - QUACK_TOKEN=${QUACK_TOKEN}
    restart: unless-stopped
```

------------------------------------------------------------------------

## Production Setup with nginx

For production, put nginx in front of Quack for TLS termination and
additional security.

### Architecture

    Client ──► HTTPS ──► nginx ──► HTTP ──► Quack Server
                          │
                    TLS + Auth headers
                    Rate limiting
                    Audit logging

### nginx Configuration

Create `/etc/nginx/sites-available/quack`:

``` nginx
server {
    listen 443 ssl http2;
    server_name duckdb.example.com;

    ssl_certificate /etc/letsencrypt/live/duckdb.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/duckdb.example.com/privkey.pem;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000" always;

    location / {
        proxy_pass http://127.0.0.1:9494;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Pass through authentication header
        proxy_set_header Authorization $http_authorization;

        # Timeouts for long queries
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
}
```

### Connecting via HTTPS

``` r

# Quack client uses HTTPS for non-localhost URIs
db_connect(
  catalog_type = "quack",
  metadata_path = "quack:duckdb.example.com/lake",
  data_path = "/data/lake",
  quack_token = "my-secret-token"
)
```

------------------------------------------------------------------------

## Cloud Deployment

### AWS Quick Start

DuckDB provides a one-click CloudFormation template:

1.  Go to [Deploying
    Quack](https://duckdb.org/docs/current/quack/setup/deployment)
2.  Click “Launch Stack” for your region
3.  Configure instance size, token, and data path
4.  Stack provisions EC2 + nginx + Let’s Encrypt automatically

### Azure

``` bash
# Create resource group
az group create --name duckdb-rg --location westeurope

# Create VM
az vm create \
  --resource-group duckdb-rg \
  --name duckdb-quack \
  --image Ubuntu2204 \
  --size Standard_D2s_v3 \
  --admin-username azureuser \
  --generate-ssh-keys

# Open port
az vm open-port --resource-group duckdb-rg --name duckdb-quack --port 9494

# SSH in and follow Linux service setup above
```

### GCP

``` bash
gcloud compute instances create duckdb-quack \
  --zone=europe-west1-b \
  --machine-type=e2-medium \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --tags=quack-server

gcloud compute firewall-rules create allow-quack \
  --allow=tcp:9494 \
  --target-tags=quack-server
```

------------------------------------------------------------------------

## Security Considerations

### Token Management

- Use strong, random tokens (32+ characters)
- Store tokens in environment variables, not scripts
- Rotate tokens periodically
- Use different tokens for different environments

``` bash
# Generate a secure token
openssl rand -base64 32
```

### Network Security

For internal networks:

- Run Quack only on internal interfaces
- Use firewall rules to restrict access
- Don’t expose port 9494 to the internet without nginx/TLS

For external access:

- Always use TLS via nginx reverse proxy
- Consider VPN or SSH tunneling
- Implement IP allowlisting if possible

### Future Security Options

Quack is designed with “bring your own security” philosophy. Future
possibilities:

| Feature        | Description                  | Status                |
|----------------|------------------------------|-----------------------|
| Token auth     | Simple shared secret         | Available             |
| Custom headers | JWT/Bearer via reverse proxy | Available (nginx)     |
| LDAP/AD        | Enterprise directory auth    | Requires custom proxy |
| mTLS           | Mutual TLS authentication    | Future                |

------------------------------------------------------------------------

## Monitoring and Troubleshooting

### Health Check

``` bash
curl http://localhost:9494/health
```

### View Connections

From DuckDB CLI on the server:

``` sql
SELECT * FROM quack_connections();
```

### Common Issues

| Issue | Solution |
|----|----|
| Connection refused | Check server running, firewall allows 9494 |
| Authentication failed | Verify token matches client and server |
| Timeout on large queries | Increase `proxy_read_timeout` in nginx |
| “Extension not found” | Run `INSTALL quack; INSTALL ducklake;` on server |
| Server crashes on startup | Check disk space, permissions on data directory |

### Logs

``` bash
# systemd logs
journalctl -u duckdb-quack -f

# nginx logs (if using)
tail -f /var/log/nginx/access.log
tail -f /var/log/nginx/error.log
```

------------------------------------------------------------------------

## When to Use Quack

**Good fit:**

- Multiple concurrent writers needed
- High-frequency streaming writes with inlining
- Edge deployment close to data
- Want DuckDB performance with concurrent access

**Not yet ready for:**

- Production workloads (wait for DuckDB 2.0)
- Environments requiring certified stability
- Cases where SQLite or PostgreSQL work fine

**Stick with SQLite/PostgreSQL when:**

- Writes are infrequent (SQLite handles this well)
- You have existing PostgreSQL infrastructure
- You need battle-tested stability now

------------------------------------------------------------------------

## Resources

- [Quack Protocol
  Documentation](https://duckdb.org/docs/current/quack/setup/overview)
- [DuckLake + Quack Integration](https://ducklake.select/docs/quack)
- [Securing Quack with Reverse
  Proxy](https://duckdb.org/docs/current/quack/setup/reverse_proxy)
- [Quack Deployment
  Guide](https://duckdb.org/docs/current/quack/setup/deployment)
