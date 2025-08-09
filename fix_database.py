#!/usr/bin/env python3
"""
Database fix script to resolve schema issues
"""

import sqlite3
import logging
import os
from database.db_manager import init_db, get_session, engine
from database.models import Base, InstagramAccount, Proxy, PublishTask

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def backup_existing_data():
    """Backup existing data from the database"""
    backup_data = {
        'accounts': [],
        'proxies': [],
        'tasks': []
    }
    
    try:
        # Connect directly to SQLite to extract any existing data
        conn = sqlite3.connect('database/bot.db')
        cursor = conn.cursor()
        
        # Check if tables exist and backup data
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            logger.info(f"Existing tables: {tables}")
            
            # Try to backup instagram_accounts if it exists
            try:
                cursor.execute("SELECT * FROM instagram_accounts")
                accounts = cursor.fetchall()
                if accounts:
                    # Get column names
                    cursor.execute("PRAGMA table_info(instagram_accounts)")
                    columns = [row[1] for row in cursor.fetchall()]
                    
                    for account in accounts:
                        account_dict = dict(zip(columns, account))
                        backup_data['accounts'].append(account_dict)
                    logger.info(f"Backed up {len(backup_data['accounts'])} accounts")
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not backup accounts: {e}")
            
            # Try to backup proxies if it exists
            try:
                cursor.execute("SELECT * FROM proxies")
                proxies = cursor.fetchall()
                if proxies:
                    cursor.execute("PRAGMA table_info(proxies)")
                    columns = [row[1] for row in cursor.fetchall()]
                    
                    for proxy in proxies:
                        proxy_dict = dict(zip(columns, proxy))
                        backup_data['proxies'].append(proxy_dict)
                    logger.info(f"Backed up {len(backup_data['proxies'])} proxies")
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not backup proxies: {e}")
                
        except Exception as e:
            logger.warning(f"Error during backup: {e}")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Failed to backup data: {e}")
    
    return backup_data

def recreate_database():
    """Recreate the database with proper schema"""
    try:
        # Remove existing database file
        if os.path.exists('database/bot.db'):
            os.remove('database/bot.db')
            logger.info("Removed old database file")
        
        # Create new database with proper schema
        init_db()
        logger.info("Created new database with proper schema")
        
        return True
    except Exception as e:
        logger.error(f"Failed to recreate database: {e}")
        return False

def restore_data(backup_data):
    """Restore backed up data to the new database"""
    try:
        session = get_session()
        
        # Restore proxies first
        for proxy_data in backup_data['proxies']:
            try:
                proxy = Proxy(
                    host=proxy_data.get('host'),
                    port=proxy_data.get('port'),
                    username=proxy_data.get('username'),
                    password=proxy_data.get('password'),
                    protocol=proxy_data.get('protocol', 'http'),
                    is_active=proxy_data.get('is_active', True)
                )
                session.add(proxy)
                logger.info(f"Restored proxy: {proxy.host}:{proxy.port}")
            except Exception as e:
                logger.error(f"Failed to restore proxy: {e}")
        
        # Restore accounts
        for account_data in backup_data['accounts']:
            try:
                account = InstagramAccount(
                    username=account_data.get('username'),
                    password=account_data.get('password'),
                    email=account_data.get('email'),
                    email_password=account_data.get('email_password'),
                    full_name=account_data.get('full_name'),
                    biography=account_data.get('biography'),
                    device_id=account_data.get('device_id'),  # This will now work
                    is_active=account_data.get('is_active', True),
                    session_data=account_data.get('session_data'),
                    proxy_id=account_data.get('proxy_id')
                )
                session.add(account)
                logger.info(f"Restored account: {account.username}")
            except Exception as e:
                logger.error(f"Failed to restore account: {e}")
        
        session.commit()
        session.close()
        
        logger.info("Successfully restored backed up data")
        return True
        
    except Exception as e:
        logger.error(f"Failed to restore data: {e}")
        return False

def fix_database():
    """Main function to fix database issues"""
    logger.info("Starting database fix process...")
    
    # Step 1: Backup existing data
    logger.info("Step 1: Backing up existing data...")
    backup_data = backup_existing_data()
    
    # Step 2: Recreate database with proper schema
    logger.info("Step 2: Recreating database with proper schema...")
    if not recreate_database():
        logger.error("Failed to recreate database")
        return False
    
    # Step 3: Restore data
    logger.info("Step 3: Restoring backed up data...")
    if not restore_data(backup_data):
        logger.error("Failed to restore data")
        return False
    
    logger.info("Database fix completed successfully!")
    return True

if __name__ == "__main__":
    fix_database() 