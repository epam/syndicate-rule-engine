from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import json

from boto3 import Session

from services.clients.resource_collectors import AWSResourceCollector

from helpers.regions import AWS_REGIONS
from executor.job import process_job_concurrent, PolicyDict, job_initializer
from helpers.constants import Cloud

def get_all_aws_policies():
    """Create discovery policies for all available AWS resources using custodian schema"""
    policies = []
    
    try:
        result = subprocess.run(['custodian', 'schema'], 
                               capture_output=True, text=True, check=True)
        
        aws_resources = []
        for line in result.stdout.split('\n'):
            line = line.strip()
            if line.startswith('- aws.'):
                resource_type = line[2:]
                aws_resources.append(resource_type)
        
        for resource_type in aws_resources:
            if resource_type in excluded_resources:
                continue
                
            clean_name = resource_type.replace('aws.', '').replace('-', '_')
            
            policy = {
                'name': f'discover-{clean_name}',
                'resource': resource_type,
                'description': f'Discover {resource_type} resources'
            }
            policies.append(PolicyDict(**policy))
            
        print(f"Generated {len(policies)} policies from custodian schema")
        return policies
        
    except subprocess.CalledProcessError as e:
        print(f"Error running custodian schema: {e}")
        fallback_resources = [
            'aws.ec2', 'aws.s3', 'aws.rds', 'aws.lambda', 
            'aws.iam-role', 'aws.security-group', 'aws.vpc'
        ]
        
        policies = []
        for resource_type in fallback_resources:
            clean_name = resource_type.replace('aws.', '').replace('-', '_')
            policy = {
                'name': f'discover-{clean_name}',
                'resource': resource_type,
                'description': f'Discover {resource_type} resources'
            }
            policies.append(PolicyDict(**policy))
            
        print(f"Using fallback policies: {len(policies)} policies")
        return policies

def count_stored_resources(work_dir):
    """Count all resources stored in work_dir by reading resources.json files"""
    work_path = Path(work_dir)
    
    if not work_path.exists():
        print(f"Work directory {work_dir} does not exist")
        return 0
    
    total_resources = 0
    resources_files = list(work_path.rglob("resources.json"))
    
    for resources_file in resources_files:
        try:
            with open(resources_file, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    count = len(data)
                    total_resources += count
                else:
                    print(f"  {resources_file.relative_to(work_path)}: Invalid format (not a list)")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"  Error reading {resources_file.relative_to(work_path)}: {e}")
    
    return total_resources

print("==============AWS Resource Collector================")

session = Session(profile_name='epmc-eoos-devops')

collector = AWSResourceCollector(session)

start = datetime.now()
resources = collector.collect_all()
end = datetime.now()

original_time = end - start
original_count = len(resources)

print("="*40)
print(f"Collection took {original_time} seconds")
print(f"Collected {original_count} resources")

print("\n==============Concurrent Cloud Custodian Collection================")

policies = get_all_aws_policies()[:5]
print(f"Generated {len(policies)} policies for all AWS resource types")

regions = AWS_REGIONS
frozen_credentials = session.get_credentials().get_frozen_credentials()

credentials = {
    'AWS_ACCESS_KEY_ID': frozen_credentials.access_key,
    'AWS_SECRET_ACCESS_KEY': frozen_credentials.secret_key,
}
if frozen_credentials.token:
    credentials['AWS_SESSION_TOKEN'] = frozen_credentials.token

concurrent_start = datetime.now()
total_successful = 0
total_failed = 0

work_dir = Path('output/aws_resources')

results = {}
with ThreadPoolExecutor(
    max_workers=10,
    initializer=job_initializer,
    initargs=(credentials,)
) as pool:
    for region in regions:    
        result = pool.submit(
            process_job_concurrent, 
            policies, work_dir, Cloud.AWS, region
        )
        results[region] = result

for region, result in results.items():
    successful, failed = result.result()
    total_successful += successful
    if failed:
        total_failed += len(failed)

concurrent_end = datetime.now()
concurrent_time = concurrent_end - concurrent_start

print("="*40)
print(f"Concurrent collection took {concurrent_time} seconds")
print(f"Total successful policies: {total_successful}")
print(f"Total failed policies: {total_failed}")
total_stored = count_stored_resources(work_dir)
print(f"Total resources stored: {total_stored}")




