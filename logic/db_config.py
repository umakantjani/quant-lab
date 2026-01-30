import os
from sqlalchemy import create_engine

# Detect if we are in the cloud (Streamlit Cloud sets this) or local
# We will set 'DATABASE_URL' in the cloud secrets later.
DB_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_z4hs0wkcylHp@ep-sweet-term-ah4drnt8-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

# Fix for some cloud providers that use 'postgres://' instead of 'postgresql://'
if DB_URL and DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

def get_engine():
    return create_engine(DB_URL)