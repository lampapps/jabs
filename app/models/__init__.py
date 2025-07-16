"""Database models for the application."""
from app.models.db_core import get_db_connection, init_db

# Ensure this runs on import to initialize the database if needed
init_db()