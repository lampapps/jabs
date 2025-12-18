"""Flask routes for the JABS monitor page: displays status of monitored targets."""

import os
import socket
import threading
from datetime import datetime, timezone

import yaml
from flask import Blueprint, render_template, request, jsonify

from app.settings import CONFIG_DIR, ENV_MODE
from app.models.discovered_instances import DiscoveredInstance
from app.utils.network_discovery import discover_jabs_instances, update_instance_status


monitor_bp = Blueprint('monitor', __name__)

@monitor_bp.route("/monitor")
def monitor():
    """Render the monitor page with status of all monitored targets."""
    monitor_yaml_path = os.path.join(CONFIG_DIR, "monitor.yaml")
    with open(monitor_yaml_path, "r", encoding="utf-8") as f:
        monitor_cfg = yaml.safe_load(f)
    shared_monitor_dir = monitor_cfg.get("shared_monitor_dir")

    # Get discovered instances from database
    discovered_instances = DiscoveredInstance.get_all()

    return render_template(
        "monitor.html",
        monitor_statuses={},  # Empty - will be populated by client-side JS
        api_statuses={},  # Empty - will be populated by client-side JS
        expected_paths={},  # Empty - will be populated by client-side JS
        problems={},  # Empty - will be populated by client-side JS
        hostname=socket.gethostname(),
        monitor_yaml_path=monitor_yaml_path,
        env_mode=ENV_MODE,
        now=datetime.now(timezone.utc).timestamp(),
        discovered_instances=discovered_instances,
        monitor_cfg=monitor_cfg,
    )


@monitor_bp.route("/api/discover_instances", methods=["POST"])
def discover_instances():
    """Trigger network discovery of JABS instances."""
    try:
        # Get configuration from monitor.yaml
        monitor_yaml_path = os.path.join(CONFIG_DIR, "monitor.yaml")
        with open(monitor_yaml_path, "r", encoding="utf-8") as f:
            monitor_cfg = yaml.safe_load(f)
        
        ip_range_start = monitor_cfg.get("ip_range_start", "192.168.1.1")
        ip_range_end = monitor_cfg.get("ip_range_end", "192.168.1.254")
        port = monitor_cfg.get("port", 5000)
        shared_monitor_dir = monitor_cfg.get("shared_monitor_dir")
        default_grace_period = monitor_cfg.get("default_grace_period", 60)
        
        # Run discovery with CLI monitoring support
        discovered_instances = discover_jabs_instances(
            ip_range_start, ip_range_end, port, 
            shared_monitor_dir=shared_monitor_dir,
            default_grace_period=default_grace_period
        )
        
        return jsonify({
            "success": True,
            "message": f"Discovery completed for IP range {ip_range_start} to {ip_range_end} on port {port}",
            "ip_range_start": ip_range_start,
            "ip_range_end": ip_range_end,
            "port": port,
            "discovered_count": len(discovered_instances)
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@monitor_bp.route("/api/discovered_instances", methods=["GET"])
def get_discovered_instances():
    """Get all discovered JABS instances from database."""
    try:
        instances = DiscoveredInstance.get_all()
        return jsonify({
            "success": True,
            "instances": [instance.to_dict() for instance in instances]
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@monitor_bp.route("/api/discovered_instances_with_status", methods=["GET"])
def get_discovered_instances_with_status():
    """Get all discovered instances with real-time status checking."""
    try:
        instances = DiscoveredInstance.get_all()
        
        # Get shared_monitor_dir from config
        monitor_yaml_path = os.path.join(CONFIG_DIR, "monitor.yaml")
        shared_monitor_dir = None
        try:
            with open(monitor_yaml_path, "r", encoding="utf-8") as f:
                monitor_cfg = yaml.safe_load(f)
            shared_monitor_dir = monitor_cfg.get("shared_monitor_dir")
        except:
            pass
        
        # Check status for each instance in real-time
        instances_with_status = []
        for instance in instances:
            instance_dict = instance.to_dict()
            
            # Get real-time status
            from app.utils.network_discovery import get_jabs_info
            jabs_info = get_jabs_info(
                instance.ip_address, 
                instance.port, 
                timeout=3, 
                shared_monitor_dir=shared_monitor_dir,
                grace_period_minutes=instance.grace_period_minutes,
                known_hostname=instance.hostname  # Pass the known hostname
            )
            
            # Add status information
            instance_dict.update({
                'flask_status': jabs_info['flask_status'],
                'cli_status': jabs_info['cli_status'],
                'flask_last_seen': jabs_info.get('flask_data', {}).get('last_scheduler_run_str'),
                'cli_last_seen': jabs_info['cli_last_seen'].isoformat() if jabs_info.get('cli_last_seen') else None,
                'status_data': {
                    'flask_data': jabs_info['flask_data'],
                    'cli_data': jabs_info['cli_data']
                },
                'error_message': None  # Could add error handling here
            })
            
            instances_with_status.append(instance_dict)
        
        return jsonify({
            "success": True,
            "instances": instances_with_status
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@monitor_bp.route("/api/discovered_instances/<int:instance_id>", methods=["DELETE"])
def delete_discovered_instance(instance_id):
    """Delete a discovered JABS instance from database."""
    try:
        success = DiscoveredInstance.delete(instance_id)
        if success:
            return jsonify({"success": True, "message": "Instance deleted successfully"})
        else:
            return jsonify({"success": False, "error": "Instance not found"}), 404
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@monitor_bp.route("/api/discovered_instances/<int:instance_id>/refresh", methods=["POST"])
def refresh_discovered_instance(instance_id):
    """Refresh status of a specific discovered instance (now just returns current status)."""
    try:
        instance = DiscoveredInstance.get_by_id(instance_id)
        if not instance:
            return jsonify({"success": False, "error": "Instance not found"}), 404
        
        return jsonify({
            "success": True, 
            "message": "Status updated (real-time)",
            "instance": instance.to_dict()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@monitor_bp.route("/api/discovered_instances/<int:instance_id>/grace_period", methods=["PUT"])
def update_instance_grace_period(instance_id):
    """Update the grace period for a specific instance."""
    try:
        instance = DiscoveredInstance.get_by_id(instance_id)
        if not instance:
            return jsonify({"success": False, "error": "Instance not found"}), 404
        
        data = request.get_json()
        if not data or 'grace_period_minutes' not in data:
            return jsonify({"success": False, "error": "grace_period_minutes is required"}), 400
        
        grace_period = int(data['grace_period_minutes'])
        if grace_period < 1:
            return jsonify({"success": False, "error": "Grace period must be at least 1 minute"}), 400
        
        instance.grace_period_minutes = grace_period
        instance.save()
        
        return jsonify({
            "success": True,
            "message": "Grace period updated successfully",
            "instance": instance.to_dict()
        })
        
    except ValueError:
        return jsonify({"success": False, "error": "Invalid grace period value"}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
