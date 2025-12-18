"""Database model for discovered JABS instances on the network."""

import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional
from app.models.db_core import get_db_connection


class DiscoveredInstance:
    """Represents a discovered JABS instance on the network."""
    
    def __init__(self, ip_address: str, hostname: str, port: int = 5000, 
                 version: str = None, last_discovered: datetime = None,
                 id: int = None, grace_period_minutes: int = 60):
        self.id = id
        self.ip_address = ip_address
        self.hostname = hostname
        self.port = port
        self.version = version
        self.last_discovered = last_discovered or datetime.utcnow()
        self.grace_period_minutes = grace_period_minutes
    
    def save(self) -> int:
        """Save or update the instance in the database."""
        with get_db_connection() as conn:
            if self.id:
                # Update existing
                conn.execute('''
                    UPDATE discovered_instances 
                    SET hostname = ?, version = ?, last_discovered = ?, grace_period_minutes = ?
                    WHERE id = ?
                ''', (self.hostname, self.version, self.last_discovered.isoformat(), 
                      self.grace_period_minutes, self.id))
                conn.commit()
                return self.id
            else:
                # Insert new
                cursor = conn.execute('''
                    INSERT INTO discovered_instances 
                    (ip_address, port, hostname, version, last_discovered, grace_period_minutes)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (self.ip_address, self.port, self.hostname, self.version,
                      self.last_discovered.isoformat(), self.grace_period_minutes))
                self.id = cursor.lastrowid
                conn.commit()
                return self.id
    
    @classmethod
    def get_all(cls) -> List['DiscoveredInstance']:
        """Get all discovered instances from the database."""
        instances = []
        with get_db_connection() as conn:
            rows = conn.execute('''
                SELECT id, ip_address, hostname, port, version, last_discovered, grace_period_minutes
                FROM discovered_instances
                ORDER BY last_discovered DESC
            ''').fetchall()
            
            for row in rows:
                last_discovered = datetime.fromisoformat(row[5]) if row[5] else datetime.utcnow()
                
                instances.append(cls(
                    id=row[0],
                    ip_address=row[1],
                    hostname=row[2],
                    port=row[3],
                    version=row[4],
                    last_discovered=last_discovered,
                    grace_period_minutes=row[6] or 60
                ))
        return instances
    
    @classmethod
    def get_by_id(cls, instance_id: int) -> Optional['DiscoveredInstance']:
        """Get a specific instance by ID."""
        with get_db_connection() as conn:
            row = conn.execute('''
                SELECT id, ip_address, hostname, port, version, last_discovered, grace_period_minutes, created_at
                FROM discovered_instances WHERE id = ?
            ''', (instance_id,)).fetchone()
            
            if not row:
                return None
            
            last_discovered = datetime.fromisoformat(row[5]) if row[5] else datetime.utcnow()
            
            return cls(
                id=row[0],
                ip_address=row[1],
                hostname=row[2],
                port=row[3],
                version=row[4],
                last_discovered=last_discovered,
                grace_period_minutes=row[6] or 60
            )
    
    @classmethod
    def delete(cls, instance_id: int) -> bool:
        """Delete an instance by ID."""
        with get_db_connection() as conn:
            cursor = conn.execute(
                'DELETE FROM discovered_instances WHERE id = ?', 
                (instance_id,)
            )
            conn.commit()
            return cursor.rowcount > 0
    
    def to_dict(self) -> Dict:
        """Convert instance to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'ip_address': self.ip_address,
            'hostname': self.hostname,
            'port': self.port,
            'version': self.version,
            'last_discovered': self.last_discovered.isoformat() if self.last_discovered else None,
            'grace_period_minutes': self.grace_period_minutes,
            'url': f"http://{self.ip_address}:{self.port}"
        }