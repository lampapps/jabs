"""Flask routes for the repository web interface."""

import os
import json
import socket
import yaml
import boto3
import botocore
from flask import Blueprint, render_template, current_app
from app.settings import ENV_MODE

repository_bp = Blueprint('repository', '__name__')

def build_local_tree(path):
    """Recursively build a tree structure for local directories and files."""
    node = {"name": os.path.basename(path) or path, "type": "directory", "children": []}
    try:
        for entry in os.scandir(path):
            if entry.is_dir(follow_symlinks=False):
                node["children"].append(build_local_tree(entry.path))
            else:
                node["children"].append({"name": entry.name, "type": "file"})
    except (OSError, PermissionError):
        pass  # Permission errors, etc.
    return node

def build_s3_tree(bucket_name, prefix="", s3_client=None):
    """Recursively build a tree structure for an S3 bucket."""
    if s3_client is None:
        s3_client = boto3.client("s3")
    node = {
        "name": bucket_name if not prefix else prefix.rstrip('/'),
        "type": "folder",
        "children": []
    }
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
            for cp in page.get('CommonPrefixes', []):
                node["children"].append(build_s3_tree(bucket_name, cp['Prefix'], s3_client))
            for obj in page.get('Contents', []):
                if obj['Key'] != prefix:
                    node["children"].append({
                        "name": os.path.basename(obj['Key']),
                        "type": "file"
                    })
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            return {
                "name": f"Error: S3 bucket '{bucket_name}' does not exist.",
                "type": "error",
                "children": []
            }
        return {
            "name": f"S3 error: {str(e)}",
            "type": "error",
            "children": []
        }
    except botocore.exceptions.BotoCoreError as e:
        return {
            "name": f"S3 error: {str(e)}",
            "type": "error",
            "children": []
        }
    return node

@repository_bp.route('/repository')
def repository():
    """Render the storage tree view for local and S3 storage."""
    config_path = os.path.join(
        os.path.dirname(current_app.root_path), 'config', 'global.yaml'
    )
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    destination = config.get("destination")
    aws_cfg = config.get("aws", {})
    bucket = aws_cfg.get("bucket")
    local_trees = []
    if destination:
        local_trees.append({
            "label": destination,
            "tree": build_local_tree(destination)
        })
    s3_trees = []
    if bucket:
        s3_client = boto3.client("s3")
        try:
            s3_trees.append({
                "label": bucket,
                "tree": build_s3_tree(bucket, s3_client=s3_client)
            })
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                s3_trees.append({
                    "label": bucket,
                    "tree": {
                        "name": f"Error: S3 bucket '{bucket}' does not exist.",
                        "type": "error",
                        "children": []
                    }
                })
            else:
                s3_trees.append({
                    "label": bucket,
                    "tree": {
                        "name": f"S3 error: {str(e)}",
                        "type": "error",
                        "children": []
                    }
                })
        except botocore.exceptions.BotoCoreError as e:
            s3_trees.append({
                "label": bucket,
                "tree": {
                    "name": f"S3 error: {str(e)}",
                    "type": "error",
                    "children": []
                }
            })
    return render_template(
        "repository.html",
        local_tree_json=json.dumps(local_trees),
        s3_tree_json=json.dumps(s3_trees),
        env_mode=ENV_MODE,
        hostname=socket.gethostname()
    )
