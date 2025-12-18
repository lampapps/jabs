#!/bin/bash

#################################################
# JABS (Just Another Backup Script)
# 
# This script handles setup, validation, and running
# of the JABS Flask application with proper environment
# management and background process control.
#################################################

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="$SCRIPT_DIR/venv"
PYTHON_VENV="$VENV_PATH/bin/python"
RUN_SCRIPT="$SCRIPT_DIR/run.py"
PID_FILE="$SCRIPT_DIR/jabs.pid"
LOG_FILE="$SCRIPT_DIR/logs/server.log"

# Color output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_header() {
    echo -e "${BLUE}[JABS]${NC} $1"
}

print_success() {
    echo -e " ${GREEN}\xE2\x9C\x93${NC} $1"
}

# Function to check Python 3.11+
check_python() {
    print_header "Checking for Python 3.11+..."
    if command -v python3 &>/dev/null; then
        PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')
        PYTHON_OK=$(python3 -c 'import sys; print(sys.version_info >= (3,11))')
        if [ "$PYTHON_OK" = "True" ]; then
            print_success "Python 3.11+ found: $PYTHON_VERSION"
        else
            print_error "Python version $PYTHON_VERSION found, but 3.11+ is required."
            echo "If Python 3.11 is not installed, you can install it on Ubuntu with:"
            echo "  sudo apt update"
            echo "  sudo apt install python3.11 python3.11-venv"
            echo "Or visit https://www.python.org/downloads/ for other platforms."
            exit 1
        fi
    else
        print_error "Python3 not found."
        exit 1
    fi
}

# Function to check venv module
check_venv_module() {
    print_header "Checking for python3.11-venv module..."
    if python3 -c "import venv" &>/dev/null; then
        print_success "python3 venv module is available."
    else
        print_error "python3 venv module is missing."
        echo "If Python 3.11 is not installed, you can install it on Ubuntu with:"
        echo "  sudo apt update"
        echo "  sudo apt install python3.11-venv"
        echo "Or visit https://www.python.org/downloads/ for other platforms."
        exit 1
    fi
}

# Function to check pip
check_pip() {
    print_header "Checking for pip..."
    if python3 -m pip --version &>/dev/null; then
        print_success "pip found."
    else
        print_error "pip not found."
        echo "You can install it on Ubuntu with:"
        echo "  sudo apt update"
        echo "  sudo apt install python3-pip"
        echo "Or visit https://pip.pypa.io/en/stable/installation/ for other platforms."
        exit 1
    fi
}

# Function to check AWS CLI
check_aws_cli() {
    print_header "Checking for AWS CLI..."
    if command -v aws &>/dev/null; then
        print_success "AWS CLI found."
    else
        print_warning "AWS CLI not found."
        echo "If AWS CLI is not installed, you can install it on Ubuntu with:"
        echo "  sudo apt update"
        echo "  sudo apt install awscli"
        echo "Or see https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html for other platforms."
    fi
}

# Function to setup config files
setup_config_files() {
    print_header "Checking configuration files..."
    
    # Check global.yaml
    if [ ! -f config/global.yaml ]; then
        if [ -f config/global-example.yaml ]; then
            print_status "Creating config/global.yaml from example..."
            if mv config/global-example.yaml config/global.yaml; then
                print_success "config/global.yaml created from global-example.yaml."
            else
                print_error "Failed to create config/global.yaml from global-example.yaml."
                echo "Please ensure you have the necessary permissions to rename files in the config directory."
                exit 1
            fi
        else
            print_error "Neither config/global.yaml nor config/global-example.yaml found."
            exit 1
        fi
    else
        print_success "config/global.yaml found."
    fi
    
    # Check monitor.yaml
    if [ ! -f config/monitor.yaml ]; then
        if [ -f config/monitor-example.yaml ]; then
            print_status "Creating config/monitor.yaml from example..."
            if mv config/monitor-example.yaml config/monitor.yaml; then
                print_success "config/monitor.yaml created from monitor-example.yaml."
            else
                print_error "Failed to create config/monitor.yaml from monitor-example.yaml."
                echo "Please ensure you have the necessary permissions to rename files in the config directory."
                exit 1
            fi
        else
            print_error "Neither config/monitor.yaml nor config/monitor-example.yaml found."
            exit 1
        fi
    else
        print_success "config/monitor.yaml found."
    fi
}

# Function to setup virtual environment
setup_virtual_env() {
    if [ -d "$VENV_PATH" ] && [ -f "$PYTHON_VENV" ]; then
        print_success "Virtual environment already exists."
        return 0
    fi
    
    print_header "Setting up virtual environment..."
    if python3 -m venv venv; then
        print_success "Virtual environment created."
    else
        print_error "Failed to create virtual environment."
        exit 1
    fi
}

# Function to install requirements
install_requirements() {
    if [ ! -f requirements.txt ]; then
        print_error "requirements.txt not found."
        echo "Please ensure you have a requirements.txt file in the current directory."
        exit 1
    fi
    
    # Check if requirements are already installed
    if [ -f "$VENV_PATH/pyvenv.cfg" ]; then
        # Simple check - if Flask is installed, assume requirements are met
        if "$PYTHON_VENV" -c "import flask" &>/dev/null; then
            print_success "Requirements already installed."
            return 0
        fi
    fi
    
    print_header "Installing requirements..."
    if "$PYTHON_VENV" -m pip install --upgrade pip && "$PYTHON_VENV" -m pip install -r requirements.txt; then
        print_success "Requirements installed."
    else
        print_error "Failed to install requirements."
        exit 1
    fi
}

# Function to validate full setup
validate_setup() {
    print_header "Validating setup..."
    
    # Check if virtual environment exists
    if [[ ! -f "$PYTHON_VENV" ]]; then
        print_error "Virtual environment not found at: $PYTHON_VENV"
        return 1
    fi
    
    # Check if run.py exists
    if [[ ! -f "$RUN_SCRIPT" ]]; then
        print_error "run.py not found at: $RUN_SCRIPT"
        return 1
    fi
    
    # Check if Flask can import
    if ! "$PYTHON_VENV" -c "import flask" &>/dev/null; then
        print_error "Flask not properly installed in virtual environment."
        return 1
    fi
    
    print_success "Setup validation complete."
    return 0
}

# Function to ensure log directory
ensure_log_dir() {
    local log_dir="$(dirname "$LOG_FILE")"
    if [[ ! -d "$log_dir" ]]; then
        mkdir -p "$log_dir"
        print_status "Created logs directory: $log_dir"
    fi
}

# Function to check if JABS is running
is_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0  # Running
        else
            # PID file exists but process is dead
            rm -f "$PID_FILE"
            return 1  # Not running
        fi
    fi
    return 1  # Not running
}

# Function to run full setup
run_setup() {
    print_header "Running JABS setup..."
    cd "$SCRIPT_DIR"
    
    check_python
    check_venv_module
    check_pip
    check_aws_cli
    setup_config_files
    setup_virtual_env
    install_requirements
    ensure_log_dir
    
    if validate_setup; then
        print_success "Setup complete! JABS is ready to use."
        echo ""
        echo "Next steps:"
        echo "  $0 start    - Start the web application"
        echo "  $0 status   - Check application status"
        echo "  $0 help     - Show all available commands"
    else
        print_error "Setup validation failed."
        exit 1
    fi
}

# Function to start JABS
start_jabs() {
    # Always validate setup before starting
    if ! validate_setup; then
        print_error "Setup validation failed. Run '$0 setup' first."
        exit 1
    fi
    
    if is_running; then
        local pid=$(cat "$PID_FILE")
        local port=$(get_jabs_port)
        print_warning "JABS is already running (PID: $pid)"
        print_status "Access at: http://localhost:$port"
        return 0
    fi
    
    # Check if the appropriate port is already in use
    local port=$(get_jabs_port)
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        print_error "Port $port is already in use by another application"
        print_error "Check what's using port $port:"
        lsof -Pi :$port -sTCP:LISTEN 2>/dev/null || netstat -tlnp | grep :$port
        print_error "Stop the conflicting application or use a different port"
        exit 1
    fi
    
    print_status "Starting JABS Flask application..."
    
    # Start the application in background
    cd "$SCRIPT_DIR"
    # Create a temporary error log to capture startup issues
    local temp_error_log="/tmp/jabs_startup_error_$$.log"
    nohup "$PYTHON_VENV" "$RUN_SCRIPT" > "$LOG_FILE" 2>"$temp_error_log" &
    local pid=$!
    
    # Save PID to file
    echo "$pid" > "$PID_FILE"
    
    # Wait a moment and check if it's still running
    sleep 3
    if ps -p "$pid" > /dev/null 2>&1; then
        local port=$(get_jabs_port)
        print_success "JABS started successfully (PID: $pid)"
        print_status "Log file: $LOG_FILE"
        print_status "Access at: http://localhost:$port"
        print_status "Use '$0 stop' to stop the application"
        # Clean up temp error log
        rm -f "$temp_error_log"
    else
        print_error "Failed to start JABS application"
        if [[ -f "$temp_error_log" && -s "$temp_error_log" ]]; then
            print_error "Startup error:"
            cat "$temp_error_log"
        fi
        print_error "Check log file: $LOG_FILE"
        rm -f "$PID_FILE" "$temp_error_log"
        exit 1
    fi
}

# Function to stop JABS
stop_jabs() {
    if ! is_running; then
        print_warning "JABS is not running"
        return 0
    fi
    
    local pid=$(cat "$PID_FILE")
    print_status "Stopping JABS application (PID: $pid)..."
    
    # Send TERM signal
    kill "$pid" 2>/dev/null
    
    # Wait for graceful shutdown
    local count=0
    while ps -p "$pid" > /dev/null 2>&1 && [[ $count -lt 10 ]]; do
        sleep 1
        ((count++))
    done
    
    # If still running, force kill
    if ps -p "$pid" > /dev/null 2>&1; then
        print_warning "Graceful shutdown failed, forcing termination..."
        kill -9 "$pid" 2>/dev/null
    fi
    
    rm -f "$PID_FILE"
    print_success "JABS application stopped"
}

# Function to restart JABS
restart_jabs() {
    print_status "Restarting JABS application..."
    stop_jabs
    sleep 1
    start_jabs
}

# Function to show status
status_jabs() {
    if is_running; then
        local pid=$(cat "$PID_FILE")
        local port=$(get_jabs_port)
        print_success "JABS is running (PID: $pid)"
        print_status "Access at: http://localhost:$port"
        print_status "Log file: $LOG_FILE"
        
        # Show recent log entries
        if [[ -f "$LOG_FILE" ]]; then
            echo ""
            echo "Recent log entries:"
            tail -5 "$LOG_FILE"
        fi
    else
        print_warning "JABS is not running"
    fi
}

# Function to show logs
show_logs() {
    if [[ -f "$LOG_FILE" ]]; then
        print_status "Showing JABS logs (Press Ctrl+C to exit):"
        tail -f "$LOG_FILE"
    else
        print_error "Log file not found: $LOG_FILE"
        print_status "Start JABS first with: $0 start"
    fi
}

# Function to reset JABS to fresh install state
reset_to_fresh_install() {
    print_header "⚠️  DANGER: Reset JABS to Fresh Install State ⚠️"
    echo ""
    print_warning "THIS WILL PERMANENTLY DELETE ALL DATA!"
    echo ""
    echo "What will be deleted:"
    echo "  • All backup job history and metadata"
    echo "  • All log files (backup, server, email, etc.)"
    echo "  • All discovered network instances"
    echo "  • All scheduler events and status"
    echo "  • All lock files and PID files"
    echo "  • Database file (jabs.sqlite)"
    echo ""
    echo "What will be preserved:"
    echo "  • Configuration files (global.yaml, monitor.yaml, job configs)"
    echo "  • Virtual environment and installed packages"
    echo "  • Backup archives in storage (not deleted)"
    echo ""
    print_warning "This action CANNOT BE UNDONE!"
    echo ""
    
    # First confirmation
    read -p "Are you absolutely sure you want to reset to fresh install? (type 'YES' to confirm): " -r
    if [[ "$REPLY" != "YES" ]]; then
        print_status "Reset cancelled."
        return 0
    fi
    
    echo ""
    print_warning "Last chance! This will delete ALL application data."
    read -p "Type 'DELETE ALL DATA' to proceed: " -r
    if [[ "$REPLY" != "DELETE ALL DATA" ]]; then
        print_status "Reset cancelled."
        return 0
    fi
    
    # Stop JABS if running
    if is_running; then
        print_status "Stopping JABS application..."
        stop_jabs
    fi
    
    print_status "Resetting JABS to fresh install state..."
    
    # Delete database
    if [[ -f "data/jabs.sqlite" ]]; then
        rm -f "data/jabs.sqlite"
        print_success "Deleted database: data/jabs.sqlite"
    fi
    
    # Delete all log files
    if [[ -d "logs" ]]; then
        find logs/ -name "*.log" -type f -delete 2>/dev/null || true
        find logs/ -name "*.status" -type f -delete 2>/dev/null || true
        print_success "Deleted all log files in logs/ directory"
    fi
    
    # Delete lock files and directories
    if [[ -d "locks" ]]; then
        rm -rf locks/*
        print_success "Deleted all lock files and directories"
    fi
    
    # Delete PID file
    if [[ -f "$PID_FILE" ]]; then
        rm -f "$PID_FILE"
        print_success "Deleted PID file"
    fi
    
    # Delete any temporary or cache files
    find . -name "*.tmp" -type f -delete 2>/dev/null || true
    find . -name "*.cache" -type f -delete 2>/dev/null || true
    find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find . -name "*.pyc" -type f -delete 2>/dev/null || true
    
    print_success "Deleted temporary and cache files"
    
    # Recreate essential directories
    mkdir -p data logs locks/restore_status
    
    # Initialize fresh database
    print_status "Initializing fresh database..."
    if "$PYTHON_VENV" -c "
from app.models.db_core import init_db
init_db()
print('Database initialized successfully')
" 2>/dev/null; then
        print_success "Fresh database initialized"
    else
        print_warning "Database initialization may need to be done on first startup"
    fi
    
    echo ""
    print_success "🎉 Reset to fresh install completed successfully!"
    echo ""
    echo "JABS has been reset to a clean state. You can now:"
    echo "  1. Start JABS: $0 start"
    echo "  2. Access web interface: http://localhost:$(get_jabs_port)"
    echo "  3. Configure new backup jobs as needed"
    echo ""
    print_warning "Remember: Your backup archives were NOT deleted."
    print_warning "You can still restore from existing backups if needed."
}

# Function to get the correct port based on ENV_MODE
get_jabs_port() {
    if [[ -f "$SCRIPT_DIR/.env" ]]; then
        if grep -q "ENV_MODE='development'" "$SCRIPT_DIR/.env" || grep -q 'ENV_MODE="development"' "$SCRIPT_DIR/.env"; then
            echo "5001"
            return
        fi
    fi
    echo "5000"
}

# Function to check if in development mode
check_development_mode() {
    if [[ -f "$SCRIPT_DIR/.env" ]]; then
        if grep -q "ENV_MODE='development'" "$SCRIPT_DIR/.env" || grep -q 'ENV_MODE="development"' "$SCRIPT_DIR/.env"; then
            return 0  # Development mode
        fi
    fi
    return 1  # Not development mode
}

# Function to update from GitHub
update_from_github() {
    print_header "Updating JABS from GitHub..."
    
    # Check for development mode
    if check_development_mode; then
        print_error "Update blocked: ENV_MODE is set to 'development' in .env file."
        print_error "This prevents accidental overwrites of development work."
        echo ""
        echo "To update anyway:"
        echo "  1. Change ENV_MODE to 'production' in .env"
        echo "  2. Commit/backup your changes first"
        echo "  3. Run update again"
        exit 1
    fi
    
    # Check if git is available
    if ! command -v git &>/dev/null; then
        print_error "Git is not installed. Please install git first:"
        echo "  sudo apt update && sudo apt install git"
        exit 1
    fi
    
    # Check if we're in a git repository
    if [[ ! -d ".git" ]]; then
        print_error "Not a git repository. Cannot update from GitHub."
        echo "This command only works if JABS was cloned from GitHub."
        exit 1
    fi
    
    # Stop the application if running
    local was_running=false
    if is_running; then
        print_status "Stopping JABS for update..."
        stop_jabs
        was_running=true
    fi
    
    # Backup current .env file
    if [[ -f ".env" ]]; then
        cp .env .env.backup.$(date +%Y%m%d_%H%M%S)
        print_status "Backed up .env file"
    fi
    
    # Save current branch
    local current_branch=$(git branch --show-current 2>/dev/null || echo "main")
    
    # Fetch latest changes
    print_status "Fetching latest changes from GitHub..."
    if ! git fetch origin; then
        print_error "Failed to fetch from GitHub. Check your internet connection."
        exit 1
    fi
    
    # Show what will be updated
    local commits_behind=$(git rev-list --count HEAD..origin/$current_branch 2>/dev/null || echo "0")
    if [[ "$commits_behind" -eq "0" ]]; then
        print_success "Already up to date with GitHub."
        if $was_running; then
            start_jabs
        fi
        return 0
    fi
    
    print_status "$commits_behind commit(s) will be pulled from GitHub"
    
    # Show recent commits
    echo ""
    echo "Recent changes:"
    git log --oneline -5 origin/$current_branch | head -5
    echo ""
    
    # Confirm update
    read -p "Continue with update? (y/N): " -r
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_status "Update cancelled."
        if $was_running; then
            start_jabs
        fi
        exit 0
    fi
    
    # Check for untracked files that would conflict
    print_status "Checking for conflicts..."
    local untracked_conflicts=$(git ls-files --others --exclude-standard | grep -E '\.(py|sh|yaml|yml|txt|md)$' | head -5)
    if [[ -n "$untracked_conflicts" ]]; then
        print_warning "Untracked files detected that may conflict:"
        echo "$untracked_conflicts" | while read file; do echo "  - $file"; done
        echo ""
        read -p "Backup and remove these files before update? (y/N): " -r
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_status "Backing up conflicting files..."
            local backup_dir="backup_$(date +%Y%m%d_%H%M%S)"
            mkdir -p "$backup_dir"
            echo "$untracked_conflicts" | while read file; do
                if [[ -f "$file" ]]; then
                    cp "$file" "$backup_dir/" 2>/dev/null || true
                    rm -f "$file"
                    print_status "Backed up and removed: $file"
                fi
            done
            print_status "Files backed up to: $backup_dir"
        else
            print_error "Cannot continue with conflicting files present."
            if $was_running; then
                start_jabs
            fi
            exit 1
        fi
    fi
    
    # Pull changes
    print_status "Pulling latest changes..."
    if ! git pull origin $current_branch; then
        print_error "Failed to pull changes. You may have local modifications."
        echo "Manual resolution steps:"
        echo "  1. git status  # Check what's modified"
        echo "  2. git stash   # Stash your changes"
        echo "  3. git pull    # Pull updates"
        echo "  4. git stash pop  # Restore your changes"
        echo ""
        echo "Or use: $0 force-update  # To automatically handle conflicts"
        exit 1
    fi
}

# Function to force update from GitHub (handles conflicts automatically)
force_update_from_github() {
    print_header "Force updating JABS from GitHub..."
    
    # Check for development mode
    if check_development_mode; then
        print_error "Update blocked: ENV_MODE is set to 'development' in .env file."
        print_error "This prevents accidental overwrites of development work."
        echo ""
        echo "To update anyway:"
        echo "  1. Change ENV_MODE to 'production' in .env"
        echo "  2. Commit/backup your changes first"
        echo "  3. Run force-update again"
        exit 1
    fi
    
    # Check if git is available
    if ! command -v git &>/dev/null; then
        print_error "Git is not installed. Please install git first:"
        echo "  sudo apt update && sudo apt install git"
        exit 1
    fi
    
    # Check if we're in a git repository
    if [[ ! -d ".git" ]]; then
        print_error "Not a git repository. Cannot update from GitHub."
        echo "This command only works if JABS was cloned from GitHub."
        exit 1
    fi
    
    # Stop the application if running
    local was_running=false
    if is_running; then
        print_status "Stopping JABS for force update..."
        stop_jabs
        was_running=true
    fi
    
    # Backup current .env file
    if [[ -f ".env" ]]; then
        cp .env .env.backup.$(date +%Y%m%d_%H%M%S)
        print_status "Backed up .env file"
    fi
    
    # Save current branch
    local current_branch=$(git branch --show-current 2>/dev/null || echo "main")
    
    # Fetch latest changes
    print_status "Fetching latest changes from GitHub..."
    if ! git fetch origin; then
        print_error "Failed to fetch from GitHub. Check your internet connection."
        exit 1
    fi
    
    # Check if there are changes to apply
    local commits_behind=$(git rev-list --count HEAD..origin/$current_branch 2>/dev/null || echo "0")
    if [[ "$commits_behind" -eq "0" ]]; then
        print_success "Already up to date with GitHub."
        if $was_running; then
            start_jabs
        fi
        return 0
    fi
    
    print_warning "FORCE UPDATE: This will automatically handle any conflicts!"
    print_status "$commits_behind commit(s) will be pulled from GitHub"
    
    # Show recent commits
    echo ""
    echo "Recent changes:"
    git log --oneline -5 origin/$current_branch | head -5
    echo ""
    
    # Handle untracked files that would conflict
    local untracked_conflicts=$(git ls-files --others --exclude-standard | grep -E '\.(py|sh|yaml|yml|txt|md)$' | head -10)
    if [[ -n "$untracked_conflicts" ]]; then
        print_status "Backing up conflicting untracked files..."
        local backup_dir="force_update_backup_$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$backup_dir"
        echo "$untracked_conflicts" | while read file; do
            if [[ -f "$file" ]]; then
                cp "$file" "$backup_dir/" 2>/dev/null || true
                rm -f "$file"
                print_status "Backed up and removed: $file"
            fi
        done
        print_status "Untracked files backed up to: $backup_dir"
    fi
    
    # Check for modified files and stash them
    if ! git diff --quiet || ! git diff --cached --quiet; then
        print_status "Stashing local changes..."
        git stash push -m "Auto-stash before force update $(date +%Y%m%d_%H%M%S)" --include-untracked
        local stashed=true
    else
        local stashed=false
    fi
    
    # Force pull changes
    print_status "Force pulling latest changes..."
    if ! git reset --hard origin/$current_branch; then
        print_error "Failed to reset to origin/$current_branch"
        exit 1
    fi
    
    # Attempt to restore stashed changes
    if $stashed; then
        print_status "Attempting to restore your stashed changes..."
        if git stash pop; then
            print_success "Stashed changes restored successfully."
        else
            print_warning "Conflict restoring stashed changes. Your changes are still in stash."
            print_status "To manually restore: git stash list && git stash apply stash@{0}"
        fi
    fi
    
    # Restore .env if it was overwritten
    local latest_env_backup=$(ls -t .env.backup.* 2>/dev/null | head -1)
    if [[ -n "$latest_env_backup" ]] && [[ -f "$latest_env_backup" ]]; then
        if ! cmp -s ".env" "$latest_env_backup" 2>/dev/null; then
            print_status "Restoring your .env file..."
            cp "$latest_env_backup" ".env"
        fi
    fi
    
    # Update requirements if changed
    if [[ -f "requirements.txt" ]]; then
        print_status "Updating Python requirements..."
        "$PYTHON_VENV" -m pip install --upgrade pip
        "$PYTHON_VENV" -m pip install -r requirements.txt
    fi
    
    print_success "Force update completed successfully!"
    
    # Restart if it was running
    if $was_running; then
        print_status "Restarting JABS..."
        start_jabs
    else
        print_status "Run '$0 start' to start JABS with the updated version."
    fi
    
    # Show version info if available
    if [[ -f "app/settings.py" ]]; then
        local version=$(grep '^VERSION = ' app/settings.py 2>/dev/null | cut -d'"' -f2 || echo "unknown")
        print_status "Updated to version: $version"
    fi
    
    # Restore .env if it was overwritten
    local latest_env_backup=$(ls -t .env.backup.* 2>/dev/null | head -1)
    if [[ -n "$latest_env_backup" ]] && [[ -f "$latest_env_backup" ]]; then
        if ! cmp -s ".env" "$latest_env_backup" 2>/dev/null; then
            print_status "Restoring your .env file..."
            cp "$latest_env_backup" ".env"
        fi
    fi
    
    # Update requirements if changed
    if [[ -f "requirements.txt" ]]; then
        print_status "Updating Python requirements..."
        "$PYTHON_VENV" -m pip install --upgrade pip
        "$PYTHON_VENV" -m pip install -r requirements.txt
    fi
    
    print_success "Update completed successfully!"
    
    # Restart if it was running
    if $was_running; then
        print_status "Restarting JABS..."
        start_jabs
    else
        print_status "Run '$0 start' to start JABS with the updated version."
    fi
    
    # Show version info if available
    if [[ -f "app/settings.py" ]]; then
        local version=$(grep '^VERSION = ' app/settings.py 2>/dev/null | cut -d'"' -f2 || echo "unknown")
        print_status "Updated to version: $version"
    fi
}

# Function to run in foreground (legacy mode)
run_foreground() {
    # Always validate setup before running
    if ! validate_setup; then
        print_error "Setup validation failed. Run '$0 setup' first."
        exit 1
    fi
    
    if is_running; then
        print_error "JABS is already running in background. Stop it first with: $0 stop"
        exit 1
    fi
    
    print_status "Running JABS in foreground mode..."
    cd "$SCRIPT_DIR"
    "$PYTHON_VENV" "$RUN_SCRIPT"
}

# Function to show usage
show_usage() {
    echo "JABS (Just Another Backup Script)"
    echo ""
    echo "Usage: $0 {command}"
    echo ""
    echo "Setup Commands:"
    echo "  setup        - Run full setup process (safe to run multiple times)"
    echo "  check        - Validate current setup without making changes"
    echo "  update       - Update JABS from GitHub (blocked in development mode)"
    echo "  force-update - Force update (stashes local changes automatically)"
    echo "  fix-update   - Fix failed update (handles conflicting files)"
    echo "  reset        - Reset to fresh install (⚠️  DELETES ALL DATA!)"
    echo ""
    echo "Application Commands:"
    echo "  start        - Start JABS web application in background"
    echo "  stop         - Stop JABS web application"
    echo "  restart      - Restart JABS web application"
    echo "  status       - Show current status and recent logs"
    echo "  run          - Run JABS in foreground (legacy mode)"
    echo ""
    echo "Monitoring Commands:"
    echo "  logs         - Follow application logs in real-time"
    echo ""
    echo "Help Commands:"
    echo "  help         - Show this help message"
    echo "  --help, -h   - Show this help message"
    echo ""
    echo "Files and Directories:"
    echo "  Virtual env:  $VENV_PATH"
    echo "  Python exec:  $PYTHON_VENV"
    echo "  Run script:   $RUN_SCRIPT"
    echo "  PID file:     $PID_FILE"
    echo "  Log file:     $LOG_FILE"
    echo "  Config dir:   $SCRIPT_DIR/config/"
    echo ""
    echo "Examples:"
    echo "  bash $0 setup         # Initial setup"
    echo "  bash $0 start         # Start web application"
    echo "  bash $0 status        # Check if running"
    echo "  bash $0 logs          # Monitor logs"
    echo "  bash $0 stop          # Stop application"
    echo "  bash $0 reset         # Reset to fresh install (⚠️  DANGER!)"
    echo ""
    echo "Web Interface:"
    echo "  After starting, access JABS at: http://localhost:$(get_jabs_port) (dev mode: 5001, prod mode: 5000)"
}

# Main execution
main() {
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Parse command
    case "${1:-help}" in
        setup)
            run_setup
            ;;
        check)
            print_header "Checking JABS setup..."
            cd "$SCRIPT_DIR"
            check_python
            check_venv_module
            check_pip
            check_aws_cli
            validate_setup
            print_success "Setup check complete."
            ;;
        update)
            update_from_github
            ;;
        force-update)
            force_update_from_github
            ;;
        fix-update)
            print_header "Fixing failed update..."
            cd "$SCRIPT_DIR"
            
            # Check if we're in a git repository
            if [[ ! -d ".git" ]]; then
                print_error "Not a git repository. Cannot fix update."
                exit 1
            fi
            
            print_status "Backing up conflicting files..."
            local backup_dir="update_conflict_backup_$(date +%Y%m%d_%H%M%S)"
            mkdir -p "$backup_dir"
            
            # Backup jabs.sh if it exists and isn't tracked
            if [[ -f "jabs.sh" ]] && ! git ls-files --error-unmatch jabs.sh >/dev/null 2>&1; then
                cp jabs.sh "$backup_dir/" 2>/dev/null || true
                rm -f jabs.sh
                print_status "Backed up and removed untracked jabs.sh"
            fi
            
            # Remove other conflicting untracked files
            git ls-files --others --exclude-standard | grep -E '\.(py|sh|yaml|yml|txt|md)$' | while read file; do
                if [[ -f "$file" ]]; then
                    cp "$file" "$backup_dir/" 2>/dev/null || true
                    rm -f "$file"
                    print_status "Backed up and removed: $file"
                fi
            done
            
            print_status "Files backed up to: $backup_dir"
            print_status "Now run: $0 update"
            ;;
        reset)
            reset_to_fresh_install
            ;;
        start)
            start_jabs
            ;;
        stop)
            stop_jabs
            ;;
        restart)
            restart_jabs
            ;;
        status)
            status_jabs
            ;;
        run)
            run_foreground
            ;;
        logs)
            show_logs
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            if [[ -n "$1" ]]; then
                print_error "Unknown command: $1"
                echo ""
            fi
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@"

