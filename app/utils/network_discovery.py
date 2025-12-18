"""Network discovery utilities for finding JABS instances on the LAN."""

import socket
import requests
import threading
import os
import logging
try:
    import ipaddress
except ImportError:
    # Fallback for older Python versions
    from ipaddr import IPv4Address as _IPv4Address
    class ipaddress:
        class IPv4Address(_IPv4Address):
            pass
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from app.models.discovered_instances import DiscoveredInstance
from app.utils.logger import setup_logger
from app.settings import ENV_MODE

# Set up discovery logger
discovery_logger = setup_logger("network_discovery", "discovery.log")


def scan_ip_port(ip: str, port: int, timeout: int = 2) -> Tuple[str, int, bool]:
    """Scan a single IP and port to check if it's open."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((ip, port))
        sock.close()
        return ip, port, result == 0
    except Exception:
        return ip, port, False


def get_jabs_info(ip: str, port: int, timeout: int = 3, shared_monitor_dir: str = None, 
                  grace_period_minutes: int = 60, known_hostname: str = None) -> Dict:
    """
    Get comprehensive JABS information including both Flask API and CLI status.
    
    Args:
        ip: IP address to check
        port: Port to check
        timeout: Request timeout in seconds
        shared_monitor_dir: Path to shared monitor directory for CLI status
        grace_period_minutes: Grace period for CLI status determination
        known_hostname: Known hostname for this instance (helps find correct JSON file)
    
    Returns a dict with:
    - flask_status: 'online'|'offline'|'unknown'
    - flask_data: dict with Flask API response data  
    - flask_last_seen: datetime object (from Flask response)
    - cli_status: 'online'|'offline'|'unknown' 
    - cli_data: dict with CLI JSON data
    - cli_last_seen: datetime object (from last_scheduler_run)
    - is_jabs: True if JABS instance detected
    - hostname: detected hostname
    - version: detected version
    """
    from app.utils.logger import setup_logger
    logger = setup_logger("network_discovery")
    logger.debug(f"get_jabs_info called: ip={ip}, port={port}, shared_monitor_dir={shared_monitor_dir}, known_hostname={known_hostname}")
    
    if ENV_MODE == 'development':
        discovery_logger.debug(f"Checking JABS info for {ip}:{port}")
        
    result = {
        'is_jabs': False,
        'hostname': '',
        'version': 'Unknown',
        'flask_status': 'unknown',
        'flask_data': {},
        'cli_status': 'unknown',
        'cli_data': {},
        'cli_last_seen': None
    }
    
    base_url = f"http://{ip}:{port}"
    
    # 1. Check Flask API status via /api/heartbeat
    try:
        response = requests.get(f"{base_url}/api/heartbeat", timeout=timeout)
        if response.status_code == 200:
            flask_data = response.json()
            result['is_jabs'] = True
            result['flask_status'] = 'online'
            result['flask_data'] = flask_data
            result['hostname'] = flask_data.get('hostname', '')
            result['version'] = flask_data.get('version', 'Unknown')
    except requests.exceptions.RequestException:
        result['flask_status'] = 'offline'
    
    # 2. If no hostname from Flask, try to resolve it
    if not result['hostname']:
        try:
            result['hostname'] = socket.gethostbyaddr(ip)[0]
        except:
            result['hostname'] = f"jabs-{ip.replace('.', '-')}"
    
    # 3. Check CLI status via monitor JSON file (if shared directory provided)
    if shared_monitor_dir and (result['hostname'] or known_hostname):
        try:
            import os
            import json
            from datetime import datetime, timezone
            from app.utils.logger import setup_logger
            logger = setup_logger("network_discovery")
            
            monitor_dir = os.path.join(shared_monitor_dir, "monitor")
            logger.debug(f"Checking CLI status for {ip}:{port}, known_hostname={known_hostname}, derived_hostname={result['hostname']}")
            
            # Try different hostname variations - prioritize known_hostname if provided
            hostnames_to_try = []
            if known_hostname:
                hostnames_to_try.append(known_hostname)
                if result['flask_data'].get('env_mode') == 'development':
                    hostnames_to_try.append(f"dev_{known_hostname}")
            
            # Add derived hostname as fallback
            if result['hostname'] and result['hostname'] not in hostnames_to_try:
                hostnames_to_try.append(result['hostname'])
                if result['flask_data'].get('env_mode') == 'development':
                    hostnames_to_try.append(f"dev_{result['hostname']}")
            
            logger.debug(f"Trying hostnames: {hostnames_to_try}")
            
            cli_found = False
            for hostname_variant in hostnames_to_try:
                json_path = os.path.join(monitor_dir, f"{hostname_variant}.json")
                logger.debug(f"Checking for JSON file: {json_path}")
                if os.path.exists(json_path):
                    try:
                        with open(json_path, "r", encoding="utf-8") as f:
                            cli_data = json.load(f)
                        result['cli_data'] = cli_data
                        result['is_jabs'] = True
                        
                        # Get last_scheduler_run timestamp for CLI status determination
                        last_scheduler_run = cli_data.get('last_scheduler_run')
                        if last_scheduler_run:
                            try:
                                # Convert timestamp to datetime for comparison
                                last_run_dt = datetime.fromtimestamp(float(last_scheduler_run), tz=timezone.utc)
                                current_time = datetime.now(timezone.utc)
                                minutes_since_last_run = (current_time - last_run_dt).total_seconds() / 60
                                
                                # CLI is online if last run is within grace period
                                if minutes_since_last_run <= grace_period_minutes:
                                    result['cli_status'] = 'online'
                                else:
                                    result['cli_status'] = 'offline'
                                    
                                # Store the actual last scheduler run time as cli_last_seen
                                result['cli_last_seen'] = last_run_dt
                                
                            except (ValueError, TypeError, OSError):
                                # Invalid timestamp
                                result['cli_status'] = 'offline'
                                result['cli_last_seen'] = None
                        else:
                            # No last_scheduler_run found
                            result['cli_status'] = 'offline'
                            result['cli_last_seen'] = None
                        
                        # Update version from CLI data if not found in Flask
                        if result['version'] == 'Unknown':
                            result['version'] = cli_data.get('version', 'Unknown')
                            
                        cli_found = True
                        break
                    except (json.JSONDecodeError, OSError):
                        continue
            
            if not cli_found:
                result['cli_status'] = 'offline' if result['is_jabs'] else 'unknown'
                result['cli_last_seen'] = None
                
        except Exception as e:
            print(f"Error checking CLI status for {ip}:{port}: {e}")
            result['cli_status'] = 'unknown'
    
    # 4. If still not identified as JABS, try legacy detection methods
    if not result['is_jabs']:
        try:
            # Try old monitor_status endpoint
            response = requests.get(f"{base_url}/api/monitor_status", timeout=timeout)
            if response.status_code == 200:
                result['is_jabs'] = True
                result['flask_status'] = 'online'
                result['version'] = 'Unknown (Legacy)'
                
            # Try to detect from web interface
            if not result['is_jabs']:
                response = requests.get(base_url, timeout=timeout)
                if response.status_code == 200:
                    content = response.text.lower()
                    if 'jabs' in content or 'just another backup script' in content:
                        result['is_jabs'] = True
                        result['flask_status'] = 'online'
                        result['version'] = 'Unknown'
                        
        except requests.exceptions.RequestException:
            pass
    
    return result


def discover_jabs_instances(ip_range_start: str, ip_range_end: str, port: int = 5000, 
                          max_workers: int = 50, shared_monitor_dir: str = None,
                          default_grace_period: int = 60) -> List[DiscoveredInstance]:
    """
    Discover JABS instances in the given IP range, checking both Flask API and CLI status.
    
    Args:
        ip_range_start: Starting IP address (e.g., "192.168.1.1")
        ip_range_end: Ending IP address (e.g., "192.168.1.254")  
        port: Port to scan (default: 5000)
        max_workers: Maximum number of concurrent threads
        shared_monitor_dir: Path to shared monitor directory for CLI status
        default_grace_period: Default grace period in minutes
    
    Returns:
        List of discovered DiscoveredInstance objects
    """
    discovery_logger.info(f"Starting discovery: IP range {ip_range_start}-{ip_range_end}, port {port}, shared_dir={shared_monitor_dir}")
    discovered_instances = []
    discovered_hostnames = set()  # Track discovered hostnames to avoid duplicates
    
    try:
        # Generate IP range
        start_ip = ipaddress.IPv4Address(ip_range_start)
        end_ip = ipaddress.IPv4Address(ip_range_end)
        
        if start_ip > end_ip:
            start_ip, end_ip = end_ip, start_ip
        
        ip_list = []
        current_ip = start_ip
        while current_ip <= end_ip:
            ip_list.append(str(current_ip))
            current_ip += 1
            
        discovery_logger.info(f"Generated {len(ip_list)} IPs to scan")
        if ENV_MODE == 'development':
            discovery_logger.debug(f"IP list: {ip_list[:5]}...{ip_list[-5:] if len(ip_list) > 10 else ip_list[5:]}")
        
        print(f"Scanning {len(ip_list)} IPs from {ip_range_start} to {ip_range_end} on port {port}")
        
        # First pass: scan for open ports
        discovery_logger.info("Phase 1: Scanning for open ports...")
        open_ports = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_ip = {executor.submit(scan_ip_port, ip, port): ip for ip in ip_list}
            
            for future in as_completed(future_to_ip):
                ip, scanned_port, is_open = future.result()
                if is_open:
                    open_ports.append((ip, scanned_port))
                    discovery_logger.info(f"Found open port: {ip}:{scanned_port}")
                    print(f"Found open port: {ip}:{scanned_port}")
        
        discovery_logger.info(f"Phase 1 complete: Found {len(open_ports)} open ports")
        print(f"Found {len(open_ports)} open ports")
        
        # Second pass: check if open ports are JABS instances
        if open_ports:
            discovery_logger.info("Phase 2: Checking open ports for JABS instances...")
            with ThreadPoolExecutor(max_workers=min(10, len(open_ports))) as executor:
                future_to_target = {executor.submit(get_jabs_info, ip, p, 3, shared_monitor_dir, default_grace_period): (ip, p) 
                                  for ip, p in open_ports}
                
                for future in as_completed(future_to_target):
                    ip, checked_port = future_to_target[future]
                    jabs_info = future.result()
                    
                    if jabs_info['is_jabs']:
                        discovery_logger.info(f"Discovered JABS instance: {ip}:{checked_port} - {jabs_info['hostname']}")
                        print(f"Discovered JABS instance: {ip}:{checked_port} - {jabs_info['hostname']}")
                        
                        # Only store discovery metadata, not status data
                        instance = DiscoveredInstance(
                            ip_address=ip,
                            hostname=jabs_info['hostname'],
                            port=checked_port,
                            version=jabs_info['version'],
                            last_discovered=datetime.utcnow(),
                            grace_period_minutes=default_grace_period
                        )
                        
                        # Save to database
                        try:
                            instance_id = instance.save()
                            discovery_logger.info(f"Saved Flask instance {jabs_info['hostname']} with ID {instance_id}")
                            discovered_instances.append(instance)
                            discovered_hostnames.add(jabs_info['hostname'])
                        except Exception as e:
                            discovery_logger.error(f"Failed to save Flask instance {jabs_info['hostname']}: {e}")
                    else:
                        if ENV_MODE == 'development':
                            discovery_logger.debug(f"Port {ip}:{checked_port} is not a JABS instance")
        else:
            discovery_logger.info("Phase 2: No open ports found, proceeding with CLI-only discovery")
            print("No open ports found, proceeding with CLI-only discovery")
        
        # Third pass: check shared monitor directory for CLI-only instances
        if shared_monitor_dir:
            discovery_logger.info("Phase 3: Checking shared monitor directory for CLI-only instances...")
            try:
                cli_only_instances = discover_cli_only_instances(
                    shared_monitor_dir, discovered_hostnames, ip_range_start, ip_range_end, 
                    port, default_grace_period
                )
                discovered_instances.extend(cli_only_instances)
                discovery_logger.info(f"Phase 3 complete: Found {len(cli_only_instances)} CLI-only instances")
            except Exception as e:
                discovery_logger.error(f"Phase 3 failed: {e}")
        else:
            discovery_logger.warning("Phase 3 skipped: No shared_monitor_dir configured")
        
        discovery_logger.info(f"Discovery complete: Found {len(discovered_instances)} total JABS instances")
        print(f"Discovery complete. Found {len(discovered_instances)} JABS instances")
        
    except Exception as e:
        discovery_logger.error(f"Discovery failed: {e}")
        print(f"Error during discovery: {e}")
    
    return discovered_instances


def discover_cli_only_instances(shared_monitor_dir: str, existing_hostnames: set, 
                               ip_range_start: str, ip_range_end: str, port: int,
                               default_grace_period: int) -> List[DiscoveredInstance]:
    """
    Discover CLI-only JABS instances by scanning the shared monitor directory for JSON files.
    
    Args:
        shared_monitor_dir: Path to shared monitor directory
        existing_hostnames: Set of hostnames already discovered via Flask API
        ip_range_start: Starting IP address for hostname-to-IP mapping
        ip_range_end: Ending IP address for hostname-to-IP mapping
        port: Port to use for the discovered instances
        default_grace_period: Default grace period in minutes
    
    Returns:
        List of discovered CLI-only DiscoveredInstance objects
    """
    import os
    import json
    from datetime import datetime, timezone
    
    discovery_logger.info(f"Starting CLI-only discovery in {shared_monitor_dir}")
    cli_instances = []
    
    try:
        monitor_dir = os.path.join(shared_monitor_dir, "monitor")
        discovery_logger.info(f"Checking monitor directory: {monitor_dir}")
        
        if not os.path.exists(monitor_dir):
            discovery_logger.warning(f"Monitor directory not found: {monitor_dir}")
            print(f"Monitor directory not found: {monitor_dir}")
            return cli_instances
        
        # Find all JSON files in monitor directory
        json_files = [f for f in os.listdir(monitor_dir) if f.endswith('.json')]
        discovery_logger.info(f"Found {len(json_files)} JSON files: {json_files}")
        print(f"Found {len(json_files)} JSON files in monitor directory")
        
        # Get existing instances from database to avoid duplicates
        from app.models.discovered_instances import DiscoveredInstance
        existing_db_instances = DiscoveredInstance.get_all()
        existing_db_hostnames = {instance.hostname for instance in existing_db_instances}
        discovery_logger.debug(f"Existing DB hostnames: {existing_db_hostnames}")
        
        for json_file in json_files:
            hostname = json_file.replace('.json', '')
            discovery_logger.info(f"Processing JSON file: {json_file} (hostname: {hostname})")
            
            # Skip if we already discovered this hostname via Flask API
            if hostname in existing_hostnames:
                discovery_logger.info(f"Skipping {hostname} - already discovered via Flask API")
                print(f"Skipping {hostname} - already discovered via Flask API")
                continue
                
            # Skip if this hostname already exists in the database
            if hostname in existing_db_hostnames:
                discovery_logger.info(f"Skipping {hostname} - already exists in database")
                print(f"Skipping {hostname} - already exists in database")
                continue
            
            json_path = os.path.join(monitor_dir, json_file)
            discovery_logger.debug(f"Reading JSON file: {json_path}")
            
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    cli_data = json.load(f)
                
                discovery_logger.debug(f"JSON data for {hostname}: {cli_data}")
                print(f"Processing CLI-only instance: {hostname}")
                
                # Try to resolve hostname to IP in our range
                instance_ip = resolve_hostname_to_ip_in_range(hostname, ip_range_start, ip_range_end)
                if not instance_ip:
                    discovery_logger.warning(f"Could not resolve {hostname} to IP in range, using hostname as IP")
                    print(f"Could not resolve {hostname} to IP in range {ip_range_start}-{ip_range_end}, using hostname")
                    instance_ip = hostname  # Use hostname if IP resolution fails
                else:
                    discovery_logger.info(f"Resolved {hostname} to IP {instance_ip}")
                
                # Get version and status from CLI data
                version = cli_data.get('version', 'Unknown')
                discovery_logger.debug(f"Version for {hostname}: {version}")
                
                # Create CLI-only instance (only store discovery metadata)
                instance = DiscoveredInstance(
                    ip_address=instance_ip,
                    hostname=hostname,
                    port=port,
                    version=version,
                    last_discovered=datetime.utcnow(),
                    grace_period_minutes=default_grace_period
                )
                
                # Save to database
                try:
                    instance_id = instance.save()
                    discovery_logger.info(f"Saved CLI-only instance {hostname} ({instance_ip}) with ID {instance_id}")
                    cli_instances.append(instance)
                    print(f"Added CLI-only instance: {hostname} ({instance_ip})")
                except Exception as save_error:
                    discovery_logger.error(f"Failed to save CLI-only instance {hostname}: {save_error}")
                    print(f"Error saving {hostname}: {save_error}")
                
            except (json.JSONDecodeError, IOError) as e:
                discovery_logger.error(f"Error reading {json_file}: {e}")
                print(f"Error reading {json_file}: {e}")
                continue
                
    except Exception as e:
        discovery_logger.error(f"Error discovering CLI-only instances: {e}")
        print(f"Error discovering CLI-only instances: {e}")
    
    discovery_logger.info(f"CLI-only discovery complete: Found {len(cli_instances)} instances")
    print(f"Discovered {len(cli_instances)} CLI-only instances")
    return cli_instances
def resolve_hostname_to_ip_in_range(hostname: str, ip_range_start: str, ip_range_end: str) -> str:
    """
    Try to resolve a hostname to an IP address and verify it's in the given range.
    
    Args:
        hostname: Hostname to resolve
        ip_range_start: Starting IP of valid range
        ip_range_end: Ending IP of valid range
        
    Returns:
        IP address string if resolved and in range, None otherwise
    """
    import socket
    
    try:
        # Try to resolve hostname to IP
        ip = socket.gethostbyname(hostname)
        
        # Check if IP is in the specified range
        start_ip = ipaddress.IPv4Address(ip_range_start)
        end_ip = ipaddress.IPv4Address(ip_range_end)
        resolved_ip = ipaddress.IPv4Address(ip)
        
        if start_ip <= resolved_ip <= end_ip:
            return ip
        else:
            print(f"Resolved IP {ip} for {hostname} is outside range {ip_range_start}-{ip_range_end}")
            return None
            
    except (socket.gaierror, socket.herror, ValueError) as e:
        print(f"Could not resolve hostname {hostname}: {e}")
        return None


def update_instance_status(instance: DiscoveredInstance, shared_monitor_dir: str = None) -> bool:
    """Update the status of a specific discovered instance (deprecated - status is now real-time)."""
    # This function is deprecated since we now use real-time status checking
    # Just update the last_discovered time
    try:
        instance.last_discovered = datetime.utcnow()
        instance.save()
        return True
    except Exception as e:
        print(f"Error updating instance {instance.ip_address}: {e}")
    
    return False