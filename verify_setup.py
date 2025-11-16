#!/usr/bin/env python3
"""
Verification script for MLOps Assignment 3 setup

This script checks if all components are properly configured.
"""

import os
import sys
import subprocess

def check_file_exists(filepath, description):
    """Check if a file exists."""
    if os.path.exists(filepath):
        print(f"✅ {description}: {filepath}")
        return True
    else:
        print(f"❌ {description} missing: {filepath}")
        return False

def check_directory_exists(dirpath, description):
    """Check if a directory exists."""
    if os.path.exists(dirpath):
        print(f"✅ {description}: {dirpath}")
        return True
    else:
        print(f"⚠️  {description} missing (will be created at runtime): {dirpath}")
        return True  # Not critical, will be created

def check_docker_running():
    """Check if Docker is running."""
    try:
        result = subprocess.run(
            ['docker', 'info'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print("✅ Docker is running")
            return True
        else:
            print("❌ Docker is not running")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("❌ Docker is not installed or not running")
        return False

def check_docker_compose():
    """Check if docker-compose is available."""
    try:
        result = subprocess.run(
            ['docker-compose', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            print(f"✅ Docker Compose: {result.stdout.strip()}")
            return True
        else:
            print("❌ Docker Compose not available")
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("❌ Docker Compose not installed")
        return False

def main():
    """Run all verification checks."""
    print("🔍 Verifying MLOps Assignment 3 Setup\n")
    print("=" * 50)
    
    all_checks_passed = True
    
    # Check Docker
    print("\n📦 Docker Environment:")
    if not check_docker_running():
        all_checks_passed = False
    if not check_docker_compose():
        all_checks_passed = False
    
    # Check required files
    print("\n📄 Required Files:")
    files_to_check = [
        ('Dockerfile', 'Dockerfile'),
        ('docker-compose.yml', 'Docker Compose configuration'),
        ('requirements.txt', 'Python requirements'),
        ('dags/nasa_apod_etl_pipeline.py', 'Airflow DAG'),
        ('plugins/nasa_apod_etl.py', 'ETL functions'),
        ('init_db.sql', 'Database initialization script'),
        ('README.md', 'Documentation'),
    ]
    
    for filepath, description in files_to_check:
        if not check_file_exists(filepath, description):
            all_checks_passed = False
    
    # Check directories (non-critical)
    print("\n📁 Directories:")
    directories_to_check = [
        ('dags', 'DAGs directory'),
        ('plugins', 'Plugins directory'),
        ('data', 'Data directory (runtime)'),
        ('dvc_repo', 'DVC repository (runtime)'),
        ('logs', 'Logs directory (runtime)'),
    ]
    
    for dirpath, description in directories_to_check:
        check_directory_exists(dirpath, description)
    
    # Summary
    print("\n" + "=" * 50)
    if all_checks_passed:
        print("\n✅ All critical checks passed!")
        print("\n📝 Next steps:")
        print("   1. Set AIRFLOW_UID: export AIRFLOW_UID=$(id -u)  # Linux/Mac")
        print("      Or: $env:AIRFLOW_UID = 50000  # Windows PowerShell")
        print("   2. Create directories: mkdir -p data dvc_repo logs")
        print("   3. Initialize Airflow: docker-compose up airflow-init")
        print("   4. Start services: docker-compose up -d")
        print("   5. Access Airflow UI at http://localhost:8080")
    else:
        print("\n❌ Some checks failed. Please fix the issues above.")
        sys.exit(1)

if __name__ == '__main__':
    main()

