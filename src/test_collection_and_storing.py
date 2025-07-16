#!/usr/bin/env python3

from unittest.mock import patch
import sys
import os
import subprocess
import json

from boto3 import Session

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.resources_collector import CustodianResourceCollector

def get_aws_credentials():
    """Get AWS credentials from boto3 session"""
    session = Session()
    creds = session.get_credentials().get_frozen_credentials()
    
    mock_credentials = {
        'aws_access_key_id': creds.access_key,
        'aws_secret_access_key': creds.secret_key,
    }
    if creds.token:
        mock_credentials['aws_session_token'] = creds.token
    
    return mock_credentials

def get_azure_credentials():
    """Get Azure credentials from Azure CLI"""
    try:
        result = subprocess.run(['az', 'account', 'show'], 
                              capture_output=True, text=True, check=True)
        account_info = json.loads(result.stdout)
        
        subprocess.run(['az', 'account', 'get-access-token'], 
                      capture_output=True, text=True, check=True)
        
        print(f"✓ Azure CLI authenticated as: {account_info.get('user', {}).get('name', 'Unknown')}")
        print(f"✓ Current subscription: {account_info['name']} ({account_info['id']})")
        
        return {
            'AZURE_SUBSCRIPTION_ID': account_info['id'],
            'AZURE_TENANT_ID': account_info['tenantId'],
        }
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error getting Azure credentials: {e}")
        print("Make sure you're logged in with 'az login'")
        return None
    except FileNotFoundError:
        print("❌ Azure CLI not found. Please install Azure CLI and run 'az login'")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing Azure CLI response: {e}")
        return None

def mock_get_credentials(tenant):
    """Mock function to return test credentials instead of real ones"""

    return get_aws_credentials()

if __name__ == '__main__':
    print("Testing Resource Collector with patched credentials...")
    print("=" * 60)
    
    with patch.object(CustodianResourceCollector, '_get_credentials', side_effect=mock_get_credentials):
        collector = CustodianResourceCollector.build()
        
        print("✓ ResourceCollector created successfully")

        print("\n" + "-" * 40)
        tenant_name = 'AWS-EPMC-EOOS'

        print(f"\nStarting resource collection for tenant: {tenant_name}")

        try:
            collector.collect_tenant_resources(
                tenant_name=tenant_name,
                regions=['us-east-1'],
            )
            print("✅ Resource collection completed successfully!")
        except Exception as e:
            print(f"❌ Resource collection failed: {e}")
    
    print("\n" + "=" * 60)
    print("Test completed!")
    print("Check your database to see if resources were stored.")