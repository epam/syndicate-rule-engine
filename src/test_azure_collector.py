from datetime import datetime

from services.clients.resource_collectors import AZUREResourceCollector

subscription_id = "dcd55c7a-80c4-4c81-b360-f15c2ac10154"

collector = AZUREResourceCollector(subscription_id)

start = datetime.now()
resources = collector.collect_all()
end = datetime.now()

print(f"Collection took {end-start} seconds")

for resource in resources[-10:]:
    print("="*40)
    print(f'Type: {resource.resource_type}')
    print(f'ID: {resource.id}')
    print(f'Name: {resource.name}')
    print(f'Location: {resource.location}')
    print(f'Data: {resource.data}')
    print(f'Sync Date: {resource.sync_date}')
