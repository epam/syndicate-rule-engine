## Table of Contents

- [Table of Contents](#table-of-contents)
- [Resources Endpoints](#resources-endpoints)
  - [Get Resources](#get-resources)
    - [Parameters](#parameters)
    - [Response](#response)
  - [Get Resource by ARN](#get-resource-by-arn)
    - [Parameters](#parameters-1)
    - [Response](#response-1)
- [Resource Exceptions Endpoints](#resource-exceptions-endpoints)
  - [List Resource Exceptions](#list-resource-exceptions)
    - [Parameters](#parameters-2)
    - [Response](#response-2)
  - [Create Resource Exception](#create-resource-exception)
    - [Request Body](#request-body)
    - [Parameters](#parameters-3)
    - [Response](#response-3)
  - [Get Resource Exception by ID](#get-resource-exception-by-id)
    - [Path Parameters](#path-parameters)
    - [Response](#response-4)
  - [Update Resource Exception](#update-resource-exception)
    - [Path Parameters](#path-parameters-1)
    - [Request Body](#request-body-1)
    - [Response](#response-5)
  - [Delete Resource Exception](#delete-resource-exception)
    - [Path Parameters](#path-parameters-2)
    - [Response](#response-6)
- [Resource Exception Types](#resource-exception-types)
  - [1. Resource-Specific Exceptions](#1-resource-specific-exceptions)
  - [2. ARN-Based Exceptions](#2-arn-based-exceptions)
  - [3. Tag-Based Exceptions](#3-tag-based-exceptions)
- [Examples](#examples)
  - [Example 1: Exclude a Development Database](#example-1-exclude-a-development-database)
  - [Example 2: Exclude All Test Resources by Tag](#example-2-exclude-all-test-resources-by-tag)
  - [Example 3: List All Exceptions for a Tenant](#example-3-list-all-exceptions-for-a-tenant)
  - [Example 4: Get Resource Details by ARN](#example-4-get-resource-details-by-arn)
- [Permissions Reference](#permissions-reference)

---

## Resources Endpoints

### Get Resources

**Endpoint:** `GET /resources`  
**Permission Required:** `resources:get`  
**Description:** Retrieve a list of cloud resources with optional filtering and pagination.

#### Parameters

| Parameter       | Type    | Required | Description                                           |
| --------------- | ------- | -------- | ----------------------------------------------------- |
| `tenant_name`   | string  | No       | Filter by specific tenant                             |
| `resource_type` | string  | No       | Filter by resource type (e.g., 'aws.ec2', 'azure.vm') |
| `location`      | string  | No       | Filter by resource location/region                    |
| `resource_id`   | string  | No       | Filter by specific resource ID                        |
| `name`          | string  | No       | Filter by resource name                               |
| `limit`         | integer | No       | Maximum number of results to return                   |
| `next_token`    | string  | No       | Token for pagination                                  |

#### Response

Returns a paginated list of resources matching the specified criteria.

```json
{
  "items": [
    {
      "id": "i-1234567890abcdef0",
      "name": "web-server-01",
      "arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
      "resource_type": "aws.ec2",
      "location": "us-east-1",
      "tenant_name": "production",
      "customer_name": "EPAM Systems",
      "sync_date": 1692451200.0,
      "data": {
        "InstanceType": "t3.micro",
        "State": "running",
        "Tags": [
          {"Key": "Environment", "Value": "production"},
          {"Key": "Owner", "Value": "team-alpha"}
        ]
      }
    }
  ],
  "next_token": "eyJ0aW1lc3RhbXAiOjE2OTI0NTEyMDB9"
}
```

---

### Get Resource by ARN

**Endpoint:** `GET /resources/arn`  
**Permission Required:** `resources:get`  
**Description:** Retrieve a specific resource by its Amazon Resource Name (ARN).

#### Parameters

| Parameter | Type   | Required | Description                                    |
| --------- | ------ | -------- | ---------------------------------------------- |
| `arn`     | string | Yes      | The Amazon Resource Name (ARN) of the resource |

#### Response

Returns detailed information about the specific resource.

```json
{
  "id": "i-1234567890abcdef0",
  "name": "web-server-01",
  "arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
  "resource_type": "aws.ec2",
  "location": "us-east-1",
  "tenant_name": "production",
  "customer_name": "EPAM Systems",
  "sync_date": 1692451200.0,
  "data": {
    "InstanceType": "t3.micro",
    "State": "running",
    "SubnetId": "subnet-12345678",
    "VpcId": "vpc-87654321"
  }
}
```

---

## Resource Exceptions Endpoints

Resource exceptions allow you to exclude specific resources from reports (for now only operational resources). There are three types of resource exceptions:

1. **Resource-specific** - Exclude a specific resource by ID, type, and location
2. **ARN-based** - Exclude resources by their AWS ARN, AZURE Id, GCP URN or K8S Id
3. **Tag-based** - Exclude resources matching specific tag filters

### List Resource Exceptions

**Endpoint:** `GET /resources/exceptions`  
**Permission Required:** `resources_exceptions:get`  
**Description:** Retrieve a list of resource exceptions with optional filtering and pagination.

#### Parameters

| Parameter       | Type          | Required | Description                         |
| --------------- | ------------- | -------- | ----------------------------------- |
| `tenant_name`   | string        | No       | Filter by specific tenant           |
| `resource_type` | string        | No       | Filter by resource type             |
| `location`      | string        | No       | Filter by resource location         |
| `resource_id`   | string        | No       | Filter by specific resource ID      |
| `arn`           | string        | No       | Filter by ARN                       |
| `tags_filters`  | array[string] | No       | Filter by tag patterns              |
| `limit`         | integer       | No       | Maximum number of results to return |
| `next_token`    | string        | No       | Token for pagination                |

#### Response

```json
{
  "items": [
    {
      "id": "exception-12345",
      "type": "resource",
      "resource_id": "i-1234567890abcdef0",
      "resource_type": "aws.ec2",
      "location": "us-east-1",
      "tenant_name": "production",
      "customer_name": "EPAM Systems",
      "created_at": 1692451200.0,
      "updated_at": 1692451200.0,
      "expire_at": "2024-08-20T12:00:00Z"
    }
  ],
  "next_token": "eyJ0aW1lc3RhbXAiOjE2OTI0NTEyMDB9"
}
```

---

### Create Resource Exception

**Endpoint:** `POST /resources/exceptions`  
**Permission Required:** `resources_exceptions:create`  
**Description:** Create a new resource exception to exclude resources from reports.

#### Request Body

You must provide **one** of the following exception types:

**Option 1: Resource-specific exception**
```json
{
  "tenant_name": "production",
  "resource_id": "i-1234567890abcdef0",
  "resource_type": "aws.ec2",
  "location": "us-east-1",
  "expire_at": "2024-12-31T23:59:59Z"
}
```

**Option 2: ARN-based exception**
```json
{
  "tenant_name": "production",
  "arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
  "expire_at": "2024-12-31T23:59:59Z"
}
```

**Option 3: Tag-based exception**
```json
{
  "customer_id": "EPAM Systems",
  "tenant_name": "production",
  "tags_filters": ["Environment=development", "Owner=team-beta"],
  "expire_at": "2024-12-31T23:59:59Z"
}
```

#### Parameters

| Parameter       | Type              | Required    | Description                                                           |
| --------------- | ----------------- | ----------- | --------------------------------------------------------------------- |
| `tenant_name`   | string            | Yes         | The tenant name                                                       |
| `resource_id`   | string            | Conditional | Resource ID (required for resource-specific exceptions)               |
| `resource_type` | string            | Conditional | Resource type (required for resource-specific exceptions)             |
| `location`      | string            | Conditional | Resource location (required for resource-specific exceptions)         |
| `arn`           | string            | Conditional | Amazon Resource Name (required for ARN-based exceptions)              |
| `tags_filters`  | array[string]     | Conditional | Tag filters in format "key=value" (required for tag-based exceptions) |
| `expire_at`     | string (ISO 8601) | Yes         | When the exception expires                                            |

#### Response

```json
{
  "id": "exception-67890",
  "type": "resource",
  "resource_id": "i-1234567890abcdef0",
  "resource_type": "aws.ec2",
  "location": "us-east-1",
  "tenant_name": "production",
  "customer_name": "EPAM Systems",
  "created_at": 1692451200.0,
  "updated_at": 1692451200.0,
  "expire_at": "2024-12-31T23:59:59Z"
}
```

---

### Get Resource Exception by ID

**Endpoint:** `GET /resources/exceptions/{id}`  
**Permission Required:** `resources_exceptions:get`  
**Description:** Retrieve a specific resource exception by its ID.

#### Path Parameters

| Parameter | Type   | Required | Description               |
| --------- | ------ | -------- | ------------------------- |
| `id`      | string | Yes      | The resource exception ID |

#### Response

```json
{
  "id": "exception-67890",
  "type": "arn",
  "arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
  "tenant_name": "production",
  "customer_name": "EPAM Systems",
  "created_at": 1692451200.0,
  "updated_at": 1692451200.0,
  "expire_at": "2024-12-31T23:59:59Z"
}
```

---

### Update Resource Exception

**Endpoint:** `PUT /resources/exceptions/{id}`  
**Permission Required:** `resources_exceptions:update`  
**Description:** Update an existing resource exception.

#### Path Parameters

| Parameter | Type   | Required | Description               |
| --------- | ------ | -------- | ------------------------- |
| `id`      | string | Yes      | The resource exception ID |

#### Request Body

The request body follows the same format as creating a resource exception:

```json
{
  "customer_id": "EPAM Systems",
  "tenant_name": "production",
  "arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-0987654321fedcba0",
  "expire_at": "2025-06-30T23:59:59Z"
}
```

#### Response

```json
{
  "id": "exception-67890",
  "type": "arn",
  "arn": "arn:aws:ec2:us-east-1:123456789012:instance/i-0987654321fedcba0",
  "tenant_name": "production",
  "customer_name": "EPAM Systems",
  "created_at": 1692451200.0,
  "updated_at": 1692537600.0,
  "expire_at": "2025-06-30T23:59:59Z"
}
```

---

### Delete Resource Exception

**Endpoint:** `DELETE /resources/exceptions/{id}`  
**Permission Required:** `resources_exceptions:delete`  
**Description:** Delete a resource exception.

#### Path Parameters

| Parameter | Type   | Required | Description               |
| --------- | ------ | -------- | ------------------------- |
| `id`      | string | Yes      | The resource exception ID |

#### Response

Returns HTTP 204 (No Content) on successful deletion.

---

## Resource Exception Types

### 1. Resource-Specific Exceptions

These exceptions target a specific resource by its ID, type, and location.

- **Use case:** Exclude a specific EC2 instance from security rules
- **Required fields:** `resource_id`, `resource_type`, `location`
- **Example:** Exclude instance `i-1234567890abcdef0` in `us-east-1`

### 2. ARN-Based Exceptions

These exceptions use the Amazon Resource Name to identify resources.

- **Use case:** Exclude resources across different regions with a single rule
- **Required fields:** `arn`
- **Example:** Exclude `arn:aws:s3:::my-bucket`
- **Note:** Also works with Google URNs and Azure resource IDs

### 3. Tag-Based Exceptions

These exceptions exclude resources based on their tags/labels.

- **Use case:** Exclude all development resources or resources owned by a specific team
- **Required fields:** `tags_filters` (array of "key=value" patterns)
- **Example:** Exclude all resources with `Environment=development`
- **Note:** Multiple filters work as AND conditions

---

## Examples

### Example 1: Exclude a Development Database

```bash
curl -X POST /resources/exceptions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "tenant_name": "development",
    "resource_id": "database-dev-01",
    "resource_type": "aws.rds",
    "location": "us-west-2",
    "expire_at": "2024-12-31T23:59:59Z"
  }'
```

### Example 2: Exclude All Test Resources by Tag

```bash
curl -X POST /resources/exceptions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "tenant_name": "staging",
    "tags_filters": ["Environment=test", "Purpose=testing"],
    "expire_at": "2024-12-31T23:59:59Z"
  }'
```

### Example 3: List All Exceptions for a Tenant

```bash
curl -X GET "/resources/exceptions?tenant_name=production" \
  -H "Authorization: Bearer $TOKEN"
```

### Example 4: Get Resource Details by ARN

```bash
curl -X GET "/resources/arn?arn=arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0" \
  -H "Authorization: Bearer $TOKEN"
```

## Permissions Reference

| Permission                    | Description                         |
| ----------------------------- | ----------------------------------- |
| `resources:get`               | View cloud resources                |
| `resources_exceptions:get`    | View resource exceptions            |
| `resources_exceptions:create` | Create new resource exceptions      |
| `resources_exceptions:update` | Modify existing resource exceptions |
| `resources_exceptions:delete` | Remove resource exceptions          |

These permissions can be assigned to roles and users through the RBAC system.
