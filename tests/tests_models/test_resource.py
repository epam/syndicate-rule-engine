from models.resource import Resource


class TestResource:
    def test_hash_changes_when_data_changes(self):
        initial_data = {"key1": "value1", "key2": "value2"}

        resource = Resource(
            id="test-id",
            name="test-resource",
            location="us-east-1",
            resource_type="EC2",
            tenant_name="test-tenant",
            customer_name="test-customer",
            data=initial_data,
            sync_date="2023-10-01T12:00:00Z"
        )
        
        initial_hash = resource.hash
        
        assert initial_hash is not None
        assert len(initial_hash) == 64
        
        modified_data = {"key1": "modified_value1", "key2": "value2"}
        resource.data = modified_data
        modified_hash = resource.hash
        
        assert modified_hash != initial_hash
        assert len(modified_hash) == 64
        
        resource.data = initial_data
        restored_hash = resource.hash
        assert restored_hash == initial_hash