from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
import subprocess
import json
import logging
from tqdm import tqdm
import sys
import time

from c7n.policy import PolicyExecutionMode, execution, utils, version, ResourceLimitExceeded

from boto3 import Session

from helpers.regions import AWS_REGIONS
from executor.job import PolicyDict, job_initializer, PoliciesLoader, Policy

from helpers.constants import Cloud

logging.getLogger('custodian').setLevel(logging.CRITICAL)
logging.getLogger('rule_engine').setLevel(logging.CRITICAL)

@execution.register('custom')
class CustomRunner(PolicyExecutionMode):

    schema = utils.type_schema('pull')

    def run(self, *args, **kw):
        if not self.policy.is_runnable():
            return []

        with self.policy.ctx as ctx:
            self.policy.log.debug(
                "Running policy:%s resource:%s region:%s c7n:%s",
                self.policy.name,
                self.policy.resource_type,
                self.policy.options.region or 'default',
                version,
            )

            s = time.time()
            try:
                resources = self.policy.resource_manager.resources()
            except ResourceLimitExceeded as e:
                self.policy.log.error(str(e))
                ctx.metrics.put_metric(
                    'ResourceLimitExceeded', e.selection_count, "Count"
                )
                raise

            rt = time.time() - s
            self.policy.log.info(
                "policy:%s resource:%s region:%s count:%d time:%0.2f",
                self.policy.name,
                self.policy.resource_type,
                self.policy.options.region,
                len(resources),
                rt,
            )
            ctx.metrics.put_metric(
                "ResourceCount", len(resources), "Count", Scope="Policy"
            )
            ctx.metrics.put_metric("ResourceTime", rt, "Seconds", Scope="Policy")
            # ctx.output.write_file('resources.json', utils.dumps(resources, indent=2))

            if not resources:
                return []

            if self.policy.options.dryrun:
                self.policy.log.debug("dryrun: skipping actions")
                return resources

            at = time.time()
            for a in self.policy.resource_manager.actions:
                s = time.time()
                with ctx.tracer.subsegment('action:%s' % a.type):
                    results = a.process(resources)
                self.policy.log.info(
                    "policy:%s action:%s"
                    " resources:%d"
                    " execution_time:%0.2f"
                    % (self.policy.name, a.name, len(resources), time.time() - s)
                )
                # if results:
                    # ctx.output.write_file("action-%s" % a.name, utils.dumps(results))
            ctx.metrics.put_metric(
                "ActionTime", time.time() - at, "Seconds", Scope="Policy"
            )
            return resources

regions = AWS_REGIONS
regions.add('global')

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
            clean_name = resource_type.replace('aws.', '').replace('-', '_')
            
            policy = {
                'name': f'discover-{clean_name}',
                'resource': resource_type,
                'description': f'Discover {resource_type} resources',
                'mode': {'type': 'custom'}
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
            
        print(f"WARNING. Using fallback policies: {len(policies)} policies")
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

def process_job(policy: Policy):
    return policy()

work_dir = Path('output')

session = Session()
frozen_credentials = session.get_credentials().get_frozen_credentials()

credentials = {
    'AWS_ACCESS_KEY_ID': frozen_credentials.access_key,
    'AWS_SECRET_ACCESS_KEY': frozen_credentials.secret_key,
}
if frozen_credentials.token:
    credentials['AWS_SESSION_TOKEN'] = frozen_credentials.token

print("Loading policies...")
start = datetime.now()
policy_loader = PoliciesLoader(
    cloud=Cloud.AWS,
    output_dir=work_dir,
    regions=regions
)

policies = policy_loader.load_from_policies(get_all_aws_policies())
end = datetime.now()
print(f"Loaded {len(policies)} policies in {end - start}")

print("Starting job processing...")
start = datetime.now()
results = []
resource_count = 0
progress_bar = tqdm(total=len(policies), desc="Processing policies", unit="policy")
with ThreadPoolExecutor(
    max_workers=10,
    initializer=job_initializer,
    initargs=(credentials,)
) as executor:
    for policy in policies:
        try:
            result = executor.submit(process_job, policy)
            results.append(result)
        except Exception as e:
            print(f"Error processing policy: {e}")

    for r in as_completed(results):
        try:
            resource_count += len(r.result())
            progress_bar.update(1)
        except Exception as e:
            print(f"Error retrieving result: {e}")
            progress_bar.update(1)

end = datetime.now()
print("="*40)
print(f"Time: {end - start}")
print(f"Total resources collected: {count_stored_resources(work_dir)}")
print(f"Total resources: {resource_count}")
for r in results[-5:]:
    try:
        resources = r.result()
        for resource in resources:
            print(f"Resource: {resource}")
    except Exception as e:
        print(f"Error retrieving result: {e}")