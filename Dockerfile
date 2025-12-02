# Multi-stage build for Extended DEX data collector
# Stage 1: Build the Rust application
FROM rust:1.91-bookworm AS builder

# Set working directory
WORKDIR /build

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build release binary
RUN cargo build --release

# Stage 2: Runtime image
FROM debian:bookworm-slim

# Install runtime dependencies (OpenSSL for native-tls)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user for running the application
# Using UID 1000 which is common for the first user on most Linux systems
RUN groupadd -r -g 1000 collector && \
    useradd -r -u 1000 -g collector -m -s /bin/bash collector

# Create app directory
WORKDIR /app

# Copy the binaries from builder
COPY --from=builder /build/target/release/collect_data /app/collect_data
COPY --from=builder /build/target/release/backtest /app/backtest
COPY --from=builder /build/target/release/grid_search /app/grid_search
COPY --from=builder /build/target/release/migrate_orderbook_to_parquet /app/migrate_orderbook_to_parquet
COPY --from=builder /build/target/release/migrate_trades_to_parquet /app/migrate_trades_to_parquet

# Create data directory and set ownership
RUN mkdir -p /app/data && \
    chown -R collector:collector /app

# Set environment variables
ENV RUST_LOG=info

# Switch to non-root user
USER collector

# Run the data collector
CMD ["/app/collect_data"]
