#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"
GENERATED_OVERRIDE="$PROJECT_ROOT/docker-compose.override.generated.yml"
DEFAULT_DB_DIR="$PROJECT_ROOT/resource/db"
DB_DIR="$DEFAULT_DB_DIR"
DEFAULT_METADATA_DB_DIR="$PROJECT_ROOT/resource/db_metadata"
METADATA_DB_DIR="$DEFAULT_METADATA_DB_DIR"
BACKUP_BASE_DIR="$PROJECT_ROOT/resource/backups"
APP_BACKUP_DIR="$BACKUP_BASE_DIR/application"
METADATA_BACKUP_DIR="$BACKUP_BASE_DIR/metadata"

if [[ -n "${DB_DATA_DIR:-}" ]]; then
  DB_DIR="$(python3 - <<'PY'
import os, sys
path = sys.argv[1]
if path.startswith("~"):
    path = os.path.expanduser(path)
print(os.path.abspath(path))
PY
"${DB_DATA_DIR}")"
fi

if [[ -n "${METADATA_DB_DATA_DIR:-}" ]]; then
  METADATA_DB_DIR="$(python3 - <<'PY'
import os, sys
path = sys.argv[1]
if path.startswith("~"):
    path = os.path.expanduser(path)
print(os.path.abspath(path))
PY
"${METADATA_DB_DATA_DIR}")"
fi

find_free_port() {
  local start_port="$1"
  python3 - <<PY
import socket
port = int($start_port)
while True:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.5)
        result = sock.connect_ex(("127.0.0.1", port))
        if result != 0:
            print(port)
            break
    port += 1
PY
}

generate_override() {
  local -a paths=("$@")
  local data_roots_json="["
  local volume_mounts_backend=""
  local volume_mounts_frontend=""
  
  for i in "${!paths[@]}"; do
    local p="${paths[$i]}"
    volume_mounts_backend="${volume_mounts_backend}      - ${p}:${p}:rw"$'\n'
    volume_mounts_frontend="${volume_mounts_frontend}      - ${p}:${p}:ro"$'\n'
    
    if [[ $i -gt 0 ]]; then
      data_roots_json="${data_roots_json},"
    fi
    data_roots_json="${data_roots_json}\"${p}\""
  done
  data_roots_json="${data_roots_json}]"
  
  cat > "$GENERATED_OVERRIDE" <<YAML
services:
  backend:
    volumes:
      - ./backend/src:/app/src
      - ./backend/pyproject.toml:/app/pyproject.toml
      - ./backend/tests:/app/tests
${volume_mounts_backend}    environment:
      DATA_ROOTS: '${data_roots_json}'
  frontend:
    volumes:
      - ./frontend:/app
${volume_mounts_frontend}    environment:
      VITE_DATA_ROOT: ${paths[0]}
      VITE_USE_REAL_FILES: "true"
YAML
}

cleanup_override() {
  if [[ -f "$GENERATED_OVERRIDE" ]]; then
    rm -f "$GENERATED_OVERRIDE"
  fi
}

cleanup_python_cache() {
  # Clean __pycache__ directories and .pyc files from backend/src
  # This prevents stale bytecode from shadowing source code changes
  local backend_src="$PROJECT_ROOT/backend/src"

  if [[ ! -d "$backend_src" ]]; then
    return 0
  fi

  echo "Cleaning Python cache from backend/src..."

  # Remove __pycache__ directories
  local pycache_count=0
  while IFS= read -r -d '' dir; do
    rm -rf "$dir" 2>/dev/null && ((pycache_count++)) || true
  done < <(find "$backend_src" -type d -name "__pycache__" -print0 2>/dev/null)

  # Remove .pyc files (in case any are outside __pycache__)
  local pyc_count=0
  while IFS= read -r -d '' file; do
    rm -f "$file" 2>/dev/null && ((pyc_count++)) || true
  done < <(find "$backend_src" -type f -name "*.pyc" -print0 2>/dev/null)

  # Remove build directories
  local build_count=0
  for pattern in "build" "*.egg-info" "*.dist-info"; do
    while IFS= read -r -d '' dir; do
      rm -rf "$dir" 2>/dev/null && ((build_count++)) || true
    done < <(find "$backend_src" -type d -name "$pattern" -print0 2>/dev/null)
  done

  # Also clean the backend root for any build artifacts
  local backend_root="$PROJECT_ROOT/backend"
  for dir in "$backend_root/build" "$backend_root"/*.egg-info; do
    if [[ -d "$dir" ]]; then
      rm -rf "$dir" 2>/dev/null && ((build_count++)) || true
    fi
  done

  echo "  Removed: $pycache_count __pycache__ dirs, $pyc_count .pyc files, $build_count build dirs"
}

cleanup_db_directory() {
  local dir="$1"
  if [[ -z "$dir" ]]; then
    return 0
  fi
  if [[ "$dir" == "/" ]]; then
    echo "Refusing to clean root directory" >&2
    return 1
  fi
  if [[ ! -d "$dir" ]]; then
    return 0
  fi

  echo "Cleaning database directory with container privileges: $dir"
  docker run --rm -v "$dir":/db busybox:1.36.1 sh -c '
set -e
for entry in /db/* /db/.[!.]* /db/..?*; do
  name="${entry##*/}"
  if [ "$name" = "*" ] || [ "$name" = "." ] || [ "$name" = ".." ]; then
    continue
  fi
  case "$name" in
    *backups)
      echo "Preserving backup directory: $name"
      continue
      ;;
  esac
  rm -rf "$entry"
done
' || return 1
  return 0
}

ensure_db_directory() {
  local dir="$1"
  mkdir -p "$dir"
  local owner_uid="${DB_UID:-$(id -u)}"
  local owner_gid="${DB_GID:-$(id -g)}"
  docker run --rm -v "$dir":/db busybox:1.36.1 sh -c "chown -R ${owner_uid}:${owner_gid} /db" >/dev/null 2>&1 || true
  chown "$owner_uid":"$owner_gid" "$dir" 2>/dev/null || true
  if [[ "$dir" == "$DEFAULT_DB_DIR" && -f "$PROJECT_ROOT/resource/db/.gitkeep" ]]; then
    touch "$dir/.gitkeep" 2>/dev/null || true
    chown "$owner_uid":"$owner_gid" "$dir/.gitkeep" 2>/dev/null || true
  elif [[ "$dir" == "$DEFAULT_METADATA_DB_DIR" && -f "$PROJECT_ROOT/resource/db_metadata/.gitkeep" ]]; then
    touch "$dir/.gitkeep" 2>/dev/null || true
    chown "$owner_uid":"$owner_gid" "$dir/.gitkeep" 2>/dev/null || true
  fi
}

ensure_backup_directory() {
  local dir="$1"
  mkdir -p "$dir"
  local owner_uid="${DB_UID:-$(id -u)}"
  local owner_gid="${DB_GID:-$(id -g)}"
  chown "$owner_uid":"$owner_gid" "$dir" 2>/dev/null || true
}

ensure_backup_directories() {
  ensure_backup_directory "$BACKUP_BASE_DIR"
  ensure_backup_directory "$APP_BACKUP_DIR"
  ensure_backup_directory "$METADATA_BACKUP_DIR"
}

usage() {
  cat <<'EOF'
Usage: scripts/manage.sh <command> [options]

Commands:
  start            Start services
  stop             Stop services
  test-frontend    Run frontend unit tests
  test-backend     Run backend unit tests

Options:
  --clean [scope]  Remove containers, volumes, and Python cache before action (start/stop only).
                   Scope may be "app", "metadata", or "both" (default).
                   Also cleans __pycache__, *.pyc, and build artifacts from backend/src.
  --data PATH      Mount PATH into containers (start only, can be specified multiple times)
  --db-dir PATH    Override database data directory (start/stop only)
  --metadata-db-dir PATH Override metadata database data directory (start/stop only)
  --no-cache-image Force docker compose build to bypass cache before starting services
  --forward        Expose ports externally (0.0.0.0) - accessible from Tailscale/network
                   Default: localhost only (127.0.0.1) - accessible only on server
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

command="$1"
shift

CLEAN=false
CLEAN_SCOPE="both"
DATA_PATHS=()
NO_CACHE_IMAGE=false
FORWARD_PORTS=false  # Default: localhost only (127.0.0.1). With --forward: external (0.0.0.0)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
      CLEAN=true
      CLEAN_SCOPE="both"
      shift
      if [[ $# -gt 0 && "$1" != -* ]]; then
        CLEAN_SCOPE="${1,,}"
        if [[ "$CLEAN_SCOPE" != "app" && "$CLEAN_SCOPE" != "metadata" && "$CLEAN_SCOPE" != "both" ]]; then
          echo "Invalid --clean scope: $CLEAN_SCOPE" >&2
          exit 1
        fi
        shift
      fi
      ;;
    --clean=*)
      CLEAN=true
      CLEAN_SCOPE="${1#--clean=}"
      CLEAN_SCOPE="${CLEAN_SCOPE,,}"
      if [[ -z "$CLEAN_SCOPE" ]]; then
        CLEAN_SCOPE="both"
      fi
      if [[ "$CLEAN_SCOPE" != "app" && "$CLEAN_SCOPE" != "metadata" && "$CLEAN_SCOPE" != "both" ]]; then
        echo "Invalid --clean scope: $CLEAN_SCOPE" >&2
        exit 1
      fi
      shift
      ;;
    --data)
      DATA_PATHS+=("$(realpath "$2")")
      shift 2
      ;;
    --db-dir)
      if [[ "$command" != "start" && "$command" != "stop" ]]; then
        echo "--db-dir is only supported with start/stop commands" >&2
        exit 1
      fi
      DB_DIR="$(python3 - <<'PY'
import os, sys
path = sys.argv[1]
if path.startswith("~"):
    path = os.path.expanduser(path)
print(os.path.abspath(path))
PY
"$2")"
      shift 2
      ;;
    --metadata-db-dir)
      if [[ "$command" != "start" && "$command" != "stop" ]]; then
        echo "--metadata-db-dir is only supported with start/stop commands" >&2
        exit 1
      fi
      METADATA_DB_DIR="$(python3 - <<'PY'
import os, sys
path = sys.argv[1]
if path.startswith("~"):
    path = os.path.expanduser(path)
print(os.path.abspath(path))
PY
"$2")"
      shift 2
      ;;
    --no-cache-image)
      NO_CACHE_IMAGE=true
      shift
      ;;
    --forward)
      FORWARD_PORTS=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

COMPOSE_ARGS=(-f "$COMPOSE_FILE")

if [[ "$command" == "start" ]]; then
  if $CLEAN; then
    # Include any previously generated override to ensure all services are torn down
    CLEAN_ARGS=(-f "$COMPOSE_FILE")
    if [[ -f "$GENERATED_OVERRIDE" ]]; then
      CLEAN_ARGS+=(-f "$GENERATED_OVERRIDE")
    fi
    export DB_DATA_DIR="$DB_DIR"
    export METADATA_DB_DATA_DIR="$METADATA_DB_DIR"
    docker compose "${CLEAN_ARGS[@]}" down -v --remove-orphans || true
    if [[ "$CLEAN_SCOPE" == "app" || "$CLEAN_SCOPE" == "both" ]]; then
      cleanup_db_directory "$DB_DIR" || {
        echo "Warning: container cleanup failed; attempting host removal" >&2
        rm -rf "$DB_DIR" || true
      }
    fi
    if [[ "$CLEAN_SCOPE" == "metadata" || "$CLEAN_SCOPE" == "both" ]]; then
      cleanup_db_directory "$METADATA_DB_DIR" || {
        echo "Warning: metadata container cleanup failed; attempting host removal" >&2
        rm -rf "$METADATA_DB_DIR" || true
      }
    fi
    # Clean Python cache to prevent stale bytecode issues
    cleanup_python_cache
  fi

  FRONTEND_PORT="$(find_free_port 5173)"
  DB_PORT="$(find_free_port 5432)"
  METADATA_DB_PORT="$(find_free_port 5532)"
  
  # Set bind address based on --forward flag
  if $FORWARD_PORTS; then
    BIND_ADDRESS="0.0.0.0"
    echo "Mode: EXTERNAL (accessible from network/Tailscale)"
  else
    BIND_ADDRESS="127.0.0.1"
    echo "Mode: LOCALHOST ONLY (server access only)"
  fi
  
  export FRONTEND_PORT DB_PORT METADATA_DB_PORT BIND_ADDRESS
  echo "Frontend: http://localhost:$FRONTEND_PORT"
  echo "Database port: $DB_PORT"
  echo "Metadata database port: $METADATA_DB_PORT"
  echo "Database directory: $DB_DIR"
  echo "Metadata database directory: $METADATA_DB_DIR"

  if [[ ${#DATA_PATHS[@]} -gt 0 ]]; then
    generate_override "${DATA_PATHS[@]}"
    COMPOSE_ARGS+=(-f "$GENERATED_OVERRIDE")
    export FRONTEND_DATA_ROOT="${DATA_PATHS[0]}"
    export VITE_USE_REAL_FILES="true"
    echo "Mounted data paths: ${DATA_PATHS[*]}"
  else
    cleanup_override
    unset FRONTEND_DATA_ROOT || true
    export VITE_USE_REAL_FILES="false"
  fi

  ensure_db_directory "$DB_DIR"
  ensure_db_directory "$METADATA_DB_DIR"
  ensure_backup_directories
  export DB_DATA_DIR="$DB_DIR"
  export METADATA_DB_DATA_DIR="$METADATA_DB_DIR"
  if $NO_CACHE_IMAGE; then
    docker compose "${COMPOSE_ARGS[@]}" build --no-cache
  fi
  docker compose "${COMPOSE_ARGS[@]}" up -d
  
  echo ""
  if $FORWARD_PORTS; then
    echo "✓ Services started - accessible from network at port $FRONTEND_PORT"
  else
    echo "✓ Services started - accessible at http://localhost:$FRONTEND_PORT (server only)"
    echo "  Use --forward to expose externally"
  fi
elif [[ "$command" == "stop" ]]; then
  if [[ -f "$GENERATED_OVERRIDE" ]]; then
    COMPOSE_ARGS+=(-f "$GENERATED_OVERRIDE")
  fi

  if $CLEAN; then
    export DB_DATA_DIR="$DB_DIR"
    export METADATA_DB_DATA_DIR="$METADATA_DB_DIR"
    docker compose "${COMPOSE_ARGS[@]}" down -v --remove-orphans || true
    if [[ "$CLEAN_SCOPE" == "app" || "$CLEAN_SCOPE" == "both" ]]; then
      cleanup_db_directory "$DB_DIR" || {
        echo "Warning: container cleanup failed; attempting host removal" >&2
        rm -rf "$DB_DIR" || true
      }
    fi
    if [[ "$CLEAN_SCOPE" == "metadata" || "$CLEAN_SCOPE" == "both" ]]; then
      cleanup_db_directory "$METADATA_DB_DIR" || {
        echo "Warning: metadata container cleanup failed; attempting host removal" >&2
        rm -rf "$METADATA_DB_DIR" || true
      }
    fi
    # Clean Python cache to prevent stale bytecode issues
    cleanup_python_cache
    ensure_db_directory "$DB_DIR"
    ensure_db_directory "$METADATA_DB_DIR"
    ensure_backup_directories
  else
    export DB_DATA_DIR="$DB_DIR"
    export METADATA_DB_DATA_DIR="$METADATA_DB_DIR"
    docker compose "${COMPOSE_ARGS[@]}" down --remove-orphans || true
  fi

  cleanup_override
  unset FRONTEND_DATA_ROOT || true
  unset VITE_USE_REAL_FILES || true
elif [[ "$command" == "test-frontend" ]]; then
  if $CLEAN || [[ ${#DATA_PATHS[@]} -gt 0 ]]; then
    echo "--clean/--data are not applicable for tests" >&2
    exit 1
  fi
  docker compose "${COMPOSE_ARGS[@]}" run --rm frontend npm run test -- --run
elif [[ "$command" == "test-backend" ]]; then
  if $CLEAN || [[ ${#DATA_PATHS[@]} -gt 0 ]]; then
    echo "--clean/--data are not applicable for tests" >&2
    exit 1
  fi
  docker compose "${COMPOSE_ARGS[@]}" run --rm --entrypoint "" backend uv run pytest tests
else
  echo "Unknown command: $command" >&2
  usage
  exit 1
fi
