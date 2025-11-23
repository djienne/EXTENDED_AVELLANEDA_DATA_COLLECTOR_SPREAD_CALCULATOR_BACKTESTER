# Docker Compose Guide for Extended DEX Data Collector

This guide covers running the Extended DEX data collection service using Docker and Docker Compose.

## Prerequisites

1. **Docker and Docker Compose installed**
   - Docker Engine 20.10+
   - Docker Compose 2.0+

2. **Configuration file ready**
   - Ensure `config.json` is configured with desired markets and settings
   - See main README or CLAUDE.md for configuration details

3. **Optional: API credentials**
   - Copy `.env.example` to `.env` and add your API key if using authenticated endpoints
   - For public data collection, `.env` is not required

4. **Permissions setup (Linux/Mac)**
   - The container runs as UID 1000 for security
   - Ensure the data directory has correct ownership before first run:
   ```bash
   mkdir -p ./data
   sudo chown -R 1000:1000 ./data
   ```
   - Windows users: Docker Desktop handles permissions automatically

## Quick Start

### 1. Build the Docker Image

```bash
docker-compose build
```

This creates a multi-stage Docker image (~100MB) with the compiled `collect_data` binary.

### 2. Start Data Collection

```bash
docker-compose up -d
```

The `-d` flag runs the container in detached mode (background).

### 3. View Logs

```bash
# Follow logs in real-time
docker-compose logs -f

# View last 100 lines
docker-compose logs --tail=100

# View logs for specific time range
docker-compose logs --since 30m
```

### 4. Stop Collection

```bash
# Graceful stop (saves state)
docker-compose stop

# Stop and remove container
docker-compose down
```

## Configuration

### config.json

Mounted at `/app/config.json` inside the container. Key settings:

```json
{
  "markets": ["ETH-USD", "BTC-USD"],
  "data_directory": "data",
  "collect_orderbook": true,
  "collect_trades": true,
  "collect_full_orderbook": true,
  "max_depth_levels": 20
}
```

**Note**: Changes to `config.json` require restarting the container:
```bash
docker-compose restart
```

### Environment Variables

Set in `.env` file or directly in `docker-compose.yml`:

- `RUST_LOG`: Logging level (default: `info`)
  - Options: `trace`, `debug`, `info`, `warn`, `error`
  - For verbose output: `RUST_LOG=debug docker-compose up`

- `EXTENDED_ENV`: Exchange environment (default: `mainnet`)
  - Options: `mainnet`, `testnet`

- `API_KEY`: API key from app.extended.exchange (optional)
  - Required for authenticated data collection
  - Set in `.env` file (not committed to git)

### Example .env File

```bash
# Copy from .env.example
cp .env.example .env

# Edit with your credentials
nano .env
```

## Data Persistence

### Data Directory

The `./data` directory is mounted as a Docker volume:
- **Host**: `./data`
- **Container**: `/app/data`

Data persists across container restarts and removals. Each market creates:
- `orderbook_depth.csv`: Full orderbook snapshots
- `trades.csv`: Public trades
- `state.json`: Resume state (last sequence, trade IDs, timestamps)

### Resume Capability

The collector maintains state in `state.json` for each market:
- **Graceful shutdown**: Press Ctrl+C or `docker-compose stop`
- **Resume**: Simply restart with `docker-compose up -d`
- **Deduplication**: Automatically skips already-collected trades and orderbook updates

## Advanced Usage

### Custom Build Arguments

Build with specific Rust version:
```bash
docker-compose build --build-arg RUST_VERSION=1.83
```

### Run with Debug Logging

```bash
RUST_LOG=debug docker-compose up
```

### Inspect Running Container

```bash
# Execute bash inside container
docker-compose exec collect_data bash

# View container stats
docker stats extended_data_collector

# Inspect container configuration
docker inspect extended_data_collector
```

### View Data Files

```bash
# List collected data
ls -lh ./data/*/

# View latest orderbook entries
tail -n 5 ./data/eth_usd/orderbook_depth.csv

# Count trades
wc -l ./data/eth_usd/trades.csv
```

## Monitoring

### Health Checks

Monitor collection progress by checking state files:

```bash
# View current state (last processed sequence, timestamp)
cat ./data/eth_usd/state.json | jq

# Check data file sizes
du -sh ./data/*/
```

### Log Patterns

Successful collection logs:
```
INFO WebSocket connected for market: ETH-USD
INFO Subscribed to orderbook_depth for ETH-USD
INFO Subscribed to public_trades for ETH-USD
INFO Saved state for ETH-USD: seq=12345, trade_id=67890
```

Error indicators:
```
ERROR WebSocket connection failed
WARN Reconnecting in 5 seconds...
```

## Troubleshooting

### Container Exits Immediately

**Issue**: Container stops right after starting.

**Solution**:
1. Check logs: `docker-compose logs`
2. Verify `config.json` exists and is valid JSON
3. Ensure data directory is writable: `chmod 777 ./data`

### File Permission Issues

**Issue**: Container cannot write to data directory, or existing CSV files have incorrect permissions.

#### Understanding Docker Permissions

The container runs as user `collector` (UID 1000, GID 1000) for security. Files created by the container will be owned by UID 1000 on the host.

**On Linux/Mac**:
- If your user UID is 1000 (check with `id -u`), no action needed
- If different, you may need to adjust permissions or use user mapping

**On Windows**:
- Docker Desktop handles permissions automatically
- Files may appear as owned by your Windows user

#### Check Current Permissions

```bash
# Check data directory ownership
ls -la ./data

# Check specific CSV file permissions
ls -la ./data/eth_usd/*.csv

# Find your user UID (Linux/Mac only)
id -u
```

#### Fix Permissions on Existing Files

**Option 1: Change ownership to UID 1000 (recommended)**
```bash
# Change ownership of data directory and all files
sudo chown -R 1000:1000 ./data

# Verify permissions
ls -la ./data/
```

**Option 2: Make files writable by all (less secure)**
```bash
# Give write permissions to all users
chmod -R a+rw ./data

# Verify permissions
ls -la ./data/
```

**Option 3: Use your host user (Linux/Mac only)**
```bash
# Edit docker-compose.yml, uncomment the user line:
# user: "${UID:-1000}:${GID:-1000}"

# Export your UID/GID and run
export UID=$(id -u)
export GID=$(id -g)
docker-compose up -d

# Change existing files to your user
sudo chown -R $UID:$GID ./data
```

#### Fix Specific CSV File Permissions

If the container cannot append to existing CSV files:

```bash
# For a specific market
sudo chown -R 1000:1000 ./data/eth_usd/*.csv
chmod 664 ./data/eth_usd/*.csv

# For all markets
find ./data -name "*.csv" -exec sudo chown 1000:1000 {} \;
find ./data -name "*.csv" -exec chmod 664 {} \;

# Also fix state.json files
find ./data -name "state.json" -exec sudo chown 1000:1000 {} \;
find ./data -name "state.json" -exec chmod 664 {} \;
```

#### Prevent Future Permission Issues

**Create data directory with correct ownership before first run**:
```bash
# Create directory
mkdir -p ./data

# Set ownership to UID 1000
sudo chown -R 1000:1000 ./data

# Set permissions: owner can read/write, others can read
chmod -R 755 ./data
```

#### Platform-Specific Notes

**Linux**:
- Most common issue: host user UID ≠ 1000
- Best fix: Use user mapping in docker-compose.yml
- Alternative: Change host files to UID 1000

**macOS**:
- Usually works out of the box
- If issues persist, try user mapping approach

**Windows (WSL2/Docker Desktop)**:
- Docker Desktop maps permissions automatically
- Rarely has permission issues
- If problems occur, ensure Docker has access to the drive in Settings

#### Verification

After fixing permissions, verify the container can write:

```bash
# Start container
docker-compose up -d

# Check logs for permission errors
docker-compose logs -f | grep -i "permission\|denied"

# Test by touching a file inside container
docker-compose exec collect_data touch /app/data/test_write.txt

# If successful, remove test file
rm ./data/test_write.txt
```

### WebSocket Connection Errors

**Issue**: Logs show "WebSocket connection failed"

**Solution**:
1. Check network connectivity: `docker-compose exec collect_data ping -c 3 google.com`
2. Verify Extended DEX API is accessible
3. If using API key, ensure `.env` file is mounted and contains valid `API_KEY`

### High Memory Usage

**Issue**: Container using excessive RAM.

**Solution**:
1. Reduce `max_depth_levels` in `config.json` (default: 20)
2. Reduce number of markets in `markets` array
3. Add memory limit to `docker-compose.yml`:
   ```yaml
   services:
     collect_data:
       mem_limit: 512m
   ```

### Data Files Growing Too Large

**Issue**: CSV files exceed multiple GB.

**Solution**:
1. Compress old data: `gzip ./data/*/orderbook_depth.csv`
2. Implement log rotation or periodic archival
3. Consider reducing collection frequency in application code

## Production Deployment

### Systemd Integration (Linux)

Create systemd service to auto-start on boot:

```bash
sudo tee /etc/systemd/system/extended-collector.service > /dev/null <<EOF
[Unit]
Description=Extended DEX Data Collector
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/path/to/extended_data
ExecStart=/usr/bin/docker-compose up -d
ExecStop=/usr/bin/docker-compose down

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable extended-collector
sudo systemctl start extended-collector
```

### Backup Strategy

Automate data backups with cron:
```bash
# Daily backup at 2 AM
0 2 * * * tar -czf /backups/extended_data_$(date +\%Y\%m\%d).tar.gz /path/to/extended_data/data
```

### Resource Limits

Add resource constraints to prevent runaway usage:

```yaml
services:
  collect_data:
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
        reservations:
          cpus: '0.5'
          memory: 256M
```

## Running Other Binaries

While `docker-compose.yml` defaults to `collect_data`, you can run other binaries:

### Backtesting

```bash
# Build backtest binary
docker build -t extended-backtest --target builder .

# Run backtest
docker run --rm \
  -v ./data:/app/data \
  -v ./config.json:/app/config.json:ro \
  extended-backtest \
  /app/target/release/backtest
```

### Verify Orderbook

```bash
docker run --rm \
  -v ./data:/app/data:ro \
  extended-backtest \
  /app/target/release/verify_orderbook
```

## Additional Resources

- **Main README**: Project overview and architecture
- **CLAUDE.md**: Developer guide and module documentation
- **config.json**: Configuration reference
- **Docker Docs**: https://docs.docker.com/compose/

## Support

For issues or questions:
- Check logs: `docker-compose logs -f`
- Review configuration: `cat config.json`
- Verify volumes: `docker volume ls`
- Inspect state: `cat ./data/*/state.json`
