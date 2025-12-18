# Installation

This guide covers how to install and run NILS on your system.

## Prerequisites

Before installing NILS, ensure you have:

- **Docker** (version 20.10 or later)
- **Docker Compose** (version 2.0 or later)
- **Git** (for cloning the repository)

### Verifying Prerequisites

```bash
# Check Docker version
docker --version
# Docker version 24.0.0 or later

# Check Docker Compose version
docker compose version
# Docker Compose version v2.20.0 or later

# Check Git
git --version
```

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/NeuroGranberg/NILS.git
cd NILS
```

### 2. Configure Environment

Copy the example environment file and adjust settings if needed:

```bash
cp .env.example .env
```

The default configuration works for most setups. See [Configuration](configuration.md) for customization options.

### 3. Start NILS

```bash
./scripts/manage.sh start
```

This command will:

1. Pull required Docker images
2. Build the application containers
3. Initialize the databases
4. Start all services

!!! info "First Start"
    The first start may take several minutes as Docker downloads and builds images.

### 4. Access the Interface

Once started, open your browser and navigate to:

```
http://localhost:5173
```

## Stopping NILS

To stop all services:

```bash
./scripts/manage.sh stop
```

## Updating NILS

To update to the latest version:

```bash
git pull
./scripts/manage.sh stop
./scripts/manage.sh start
```

## Troubleshooting

### Port Conflicts

If port 5173 is already in use, you can modify the port in your `.env` file:

```bash
FRONTEND_PORT=8080
```

### Permission Issues

On Linux, you may need to add your user to the docker group:

```bash
sudo usermod -aG docker $USER
# Log out and back in for changes to take effect
```

### Container Issues

To clean up and start fresh:

```bash
./scripts/manage.sh stop --clean
./scripts/manage.sh start
```

## Next Steps

- Continue to [Quick Start](quick-start.md) to import your first dataset
- See [Configuration](configuration.md) for advanced settings
