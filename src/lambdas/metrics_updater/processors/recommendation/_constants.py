from helpers.constants import Cloud
from enum import Enum


SOURCE = "SYNDICATE_RULE_ENGINE"
DEFAULT_DESCRIPTION = "Description"

class MCCResourceType(str, Enum):
    """
    Resource types for MCC.
    """

    UNKNOWN = "UNKNOWN"
    
    # Compute resources
    INSTANCE = "INSTANCE"  # Virtual machines, EC2, VM, Compute instances
    FUNCTION = "FUNCTION"  # Serverless functions: Lambda, Cloud Functions, Azure Functions
    CONTAINER_SERVICE = "CONTAINER_SERVICE"  # ECS services, AKS, container orchestration services
    CONTAINER_REGISTRY = "CONTAINER_REGISTRY"  # ECR, ACR, GCR
    IMAGE = "IMAGE"  # Machine images: AMI, VM images, container images
    
    # Storage resources
    VOLUME = "VOLUME"  # Block storage: EBS, disks
    BUCKET = "BUCKET"  # Object storage: S3, Blob Storage, Cloud Storage
    FILE_SYSTEM = "FILE_SYSTEM"  # Network file systems: EFS, FSx, Azure Files
    SNAPSHOT = "SNAPSHOT"  # Snapshots of volumes and disks
    BACKUP = "BACKUP"  # Backup vaults, plans, and backup storage
    
    # Database resources
    DATABASE = "DATABASE"  # All database types: RDS, DynamoDB, CosmosDB, SQL, NoSQL
    CACHE = "CACHE"  # Cache services: ElastiCache, Redis, Memcached, DAX
    
    # Networking resources
    VPC = "VPC"  # Virtual networks: VPC, VNet
    SUBNET = "SUBNET"  # Subnets within virtual networks
    SECURITY_GROUP = "SECURITY_GROUP"  # Security groups and firewall rules
    LOAD_BALANCER = "LOAD_BALANCER"  # Load balancers: ELB, ALB, NLB, Azure LB
    NETWORK_INTERFACE = "NETWORK_INTERFACE"  # ENI, network interfaces
    NETWORK_GATEWAY = "NETWORK_GATEWAY"  # NAT Gateway, Internet Gateway, VPN Gateway, Transit Gateway
    IP_ADDRESS = "IP_ADDRESS"  # Elastic IPs, Public IPs, static addresses
    DNS_ZONE = "DNS_ZONE"  # Route53 zones, DNS zones
    CDN = "CDN"  # CloudFront, CDN distributions
    REGION = "REGION"  # Regions, zones
    ZONE = "ZONE"  # Zones
    
    # Security & Identity resources
    IAM_USER = "IAM_USER"  # IAM users, service principals
    IAM_ROLE = "IAM_ROLE"  # IAM roles, managed identities
    IAM_GROUP = "IAM_GROUP"  # IAM groups
    IAM_POLICY = "IAM_POLICY"  # IAM policies, role definitions
    ENCRYPTION_KEY = "ENCRYPTION_KEY"  # KMS keys, Key Vault keys
    SECRET = "SECRET"  # Secrets Manager, Key Vault secrets, Secret Manager
    CERTIFICATE = "CERTIFICATE"  # SSL/TLS certificates: ACM, Key Vault certs
    
    # Application & Integration resources
    API_GATEWAY = "API_GATEWAY"  # API Gateway, API Management
    MESSAGE_QUEUE = "MESSAGE_QUEUE"  # SQS, SNS, EventHub, Kafka, PubSub, Service Bus
    EVENT_BUS = "EVENT_BUS"  # EventBridge, Event Grid
    CONFIG = "CONFIG"  # Configuration, App Configuration
    
    # Management & Orchestration resources
    K8S_CLUSTER = "K8S_CLUSTER"  # EKS, AKS, GKE clusters
    AUTOSCALING_GROUP = "AUTOSCALING_GROUP"  # Auto Scaling Groups, Scale Sets
    
    # Infrastructure as Code
    TERRAFORM_TEMPLATE = "TERRAFORM_TEMPLATE"  # CloudFormation templates, ARM templates
    TERRAFORM_STACK = "TERRAFORM_STACK"  # CloudFormation stacks, deployed templates
    TEMPLATE = "TEMPLATE"  # Generic templates
    STACK = "STACK"  # Generic stacks
    
    # Organization resources
    TENANT = "TENANT"  # AWS accounts, Azure subscriptions, GCP projects
    
    # Monitoring & Logging resources
    LOG_GROUP = "LOG_GROUP"  # CloudWatch log groups, Log Analytics workspaces


class K8SMCCResourceType(str, Enum):
    """
    Resource types for Kubernetes.
    """

    UNKNOWN = "UNKNOWN"
    ROLE = "ROLE"
    CONFIG = "CONFIG"
    DEPLOYMENT = "DEPLOYMENT"
    NAMESPACE = "NAMESPACE"
    POD = "POD"
    SECRET = "SECRET"
    SERVICE_ACCOUNT = "SERVICE_ACCOUNT"


_K8S_MAP: dict[str, K8SMCCResourceType] = {
    "k8s.cluster-role": K8SMCCResourceType.ROLE,
    "k8s.config-map": K8SMCCResourceType.CONFIG,
    "k8s.deployment": K8SMCCResourceType.DEPLOYMENT,
    "k8s.namespace": K8SMCCResourceType.NAMESPACE,
    "k8s.pod": K8SMCCResourceType.POD,
    "k8s.role": K8SMCCResourceType.ROLE,
    "k8s.secret": K8SMCCResourceType.SECRET,
    "k8s.service-account": K8SMCCResourceType.SERVICE_ACCOUNT,

    # Others
    "k8s.cluster-role-binding": K8SMCCResourceType.ROLE,
    "k8s.role-binding": K8SMCCResourceType.ROLE,
    "k8s.network-policy": K8SMCCResourceType.CONFIG,
    "k8s.persistent-volume": K8SMCCResourceType.CONFIG,
    "k8s.persistent-volume-claim": K8SMCCResourceType.CONFIG,
    "k8s.service": K8SMCCResourceType.CONFIG,
    "k8s.daemonset": K8SMCCResourceType.DEPLOYMENT,
    "k8s.statefulset": K8SMCCResourceType.DEPLOYMENT,
    "k8s.replicaset": K8SMCCResourceType.DEPLOYMENT,
    "k8s.job": K8SMCCResourceType.DEPLOYMENT,
    "k8s.cronjob": K8SMCCResourceType.DEPLOYMENT,
}

RESOURCE_TYPE_MAPPING: dict[Cloud, dict[str, MCCResourceType] | dict[str, K8SMCCResourceType]] = {
    Cloud.AWS: {
        # Account & Organization
        "account": MCCResourceType.TENANT,
        "aws.account": MCCResourceType.TENANT,
        
        # Compute
        "ami": MCCResourceType.IMAGE,
        "aws.ami": MCCResourceType.IMAGE,
        "aws.ec2": MCCResourceType.INSTANCE,
        "aws.ec2-reserved": MCCResourceType.INSTANCE,
        "ec2": MCCResourceType.INSTANCE,
        "aws.lightsail-instance": MCCResourceType.INSTANCE,
        "aws.lambda": MCCResourceType.FUNCTION,
        "lambda": MCCResourceType.FUNCTION,
        "aws.elasticbeanstalk-environment": MCCResourceType.INSTANCE,
        "aws.emr": MCCResourceType.INSTANCE,
        "emr": MCCResourceType.INSTANCE,
        "aws.workspaces": MCCResourceType.INSTANCE,
        
        # Containers
        "aws.ecr": MCCResourceType.CONTAINER_REGISTRY,
        "ecr": MCCResourceType.CONTAINER_REGISTRY,
        "aws.ecs": MCCResourceType.CONTAINER_SERVICE,
        "aws.ecs-service": MCCResourceType.CONTAINER_SERVICE,
        "ecs": MCCResourceType.CONTAINER_SERVICE,
        "ecs-service": MCCResourceType.CONTAINER_SERVICE,
        "aws.eks": MCCResourceType.K8S_CLUSTER,
        "eks": MCCResourceType.K8S_CLUSTER,
        
        # Storage
        "aws.ebs": MCCResourceType.VOLUME,
        "aws.ebs-snapshot": MCCResourceType.SNAPSHOT,
        "ebs-snapshot": MCCResourceType.SNAPSHOT,
        "aws.efs": MCCResourceType.FILE_SYSTEM,
        "efs": MCCResourceType.FILE_SYSTEM,
        "aws.fsx": MCCResourceType.FILE_SYSTEM,
        "aws.fsx-volume": MCCResourceType.VOLUME,
        "aws.fsx-backup": MCCResourceType.BACKUP,
        "aws.s3": MCCResourceType.BUCKET,
        "s3": MCCResourceType.BUCKET,
        "aws.glacier": MCCResourceType.BACKUP,
        "aws.backup-plan": MCCResourceType.BACKUP,
        "aws.backup-vault": MCCResourceType.BACKUP,
        
        # Database
        "aws.dynamodb-table": MCCResourceType.DATABASE,
        "dynamodb-table": MCCResourceType.DATABASE,
        "aws.rds": MCCResourceType.DATABASE,
        "aws.rds-cluster": MCCResourceType.DATABASE,
        "aws.rds-reserved": MCCResourceType.DATABASE,
        "rds": MCCResourceType.DATABASE,
        "rds-cluster": MCCResourceType.DATABASE,
        "aws.rds-snapshot": MCCResourceType.SNAPSHOT,
        "rds-snapshot": MCCResourceType.SNAPSHOT,
        "aws.redshift-reserved": MCCResourceType.DATABASE,
        "redshift": MCCResourceType.DATABASE,
        "aws.elasticsearch": MCCResourceType.DATABASE,
        "elasticsearch": MCCResourceType.DATABASE,
        "elasticsearch-reserved": MCCResourceType.DATABASE,
        "aws.dax": MCCResourceType.CACHE,
        "dax": MCCResourceType.CACHE,
        "qldb": MCCResourceType.DATABASE,
        "aws.dms-instance": MCCResourceType.INSTANCE,
        "dms-instance": MCCResourceType.INSTANCE,
        
        # Cache
        "aws.cache-cluster": MCCResourceType.CACHE,
        "cache-cluster": MCCResourceType.CACHE,
        "aws.elasticache-group": MCCResourceType.CACHE,
        "elasticache-group": MCCResourceType.CACHE,
        
        # Networking
        "aws.vpc": MCCResourceType.VPC,
        "vpc": MCCResourceType.VPC,
        "aws.subnet": MCCResourceType.SUBNET,
        "subnet": MCCResourceType.SUBNET,
        "aws.security-group": MCCResourceType.SECURITY_GROUP,
        "security-group": MCCResourceType.SECURITY_GROUP,
        "aws.eni": MCCResourceType.NETWORK_INTERFACE,
        "aws.nat-gateway": MCCResourceType.NETWORK_GATEWAY,
        "nat-gateway": MCCResourceType.NETWORK_GATEWAY,
        "internet-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.transit-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.transit-attachment": MCCResourceType.NETWORK_GATEWAY,
        "vpn-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.vpn-connection": MCCResourceType.NETWORK_GATEWAY,
        "elastic-ip": MCCResourceType.IP_ADDRESS,
        "network-addr": MCCResourceType.IP_ADDRESS,
        "aws.vpc-endpoint": MCCResourceType.NETWORK_GATEWAY,
        "vpc-endpoint-service-configuration": MCCResourceType.NETWORK_GATEWAY,
        "aws.network-acl": MCCResourceType.SECURITY_GROUP,
        "aws.peering-connection": MCCResourceType.NETWORK_GATEWAY,
        
        # Load Balancing
        "app-elb": MCCResourceType.LOAD_BALANCER,
        "aws.app-elb": MCCResourceType.LOAD_BALANCER,
        "aws.elb": MCCResourceType.LOAD_BALANCER,
        "elb": MCCResourceType.LOAD_BALANCER,
        
        # DNS & CDN
        "aws.hostedzone": MCCResourceType.DNS_ZONE,
        "aws.r53domain": MCCResourceType.DNS_ZONE,
        "aws.rrset": MCCResourceType.DNS_ZONE,
        "aws.distribution": MCCResourceType.CDN,
        "distribution": MCCResourceType.CDN,
        
        # Security & Identity
        "acm-certificate": MCCResourceType.CERTIFICATE,
        "aws.acm-certificate": MCCResourceType.CERTIFICATE,
        "iam-certificate": MCCResourceType.CERTIFICATE,
        "aws.iam-user": MCCResourceType.IAM_USER,
        "iam-user": MCCResourceType.IAM_USER,
        "aws.iam-group": MCCResourceType.IAM_GROUP,
        "iam-role": MCCResourceType.IAM_ROLE,
        "iam-policy": MCCResourceType.IAM_POLICY,
        "aws.kms-key": MCCResourceType.ENCRYPTION_KEY,
        "aws.secrets-manager": MCCResourceType.SECRET,
        "key-pair": MCCResourceType.SECRET,
        "aws.key-pair": MCCResourceType.SECRET,
        "aws.waf": MCCResourceType.SECURITY_GROUP,
        "aws.waf-regional": MCCResourceType.SECURITY_GROUP,
        "aws.iam-role": MCCResourceType.IAM_ROLE,
        
        # API & Integration
        "aws.apigwv2-stage": MCCResourceType.API_GATEWAY,
        "aws.graphql-api": MCCResourceType.API_GATEWAY,
        "graphql-api": MCCResourceType.API_GATEWAY,
        "rest-api": MCCResourceType.API_GATEWAY,
        "rest-resource": MCCResourceType.API_GATEWAY,
        "rest-stage": MCCResourceType.API_GATEWAY,
        
        # Messaging & Events
        "aws.sns": MCCResourceType.MESSAGE_QUEUE,
        "sns": MCCResourceType.MESSAGE_QUEUE,
        "sqs": MCCResourceType.MESSAGE_QUEUE,
        "aws.kafka": MCCResourceType.MESSAGE_QUEUE,
        "kafka": MCCResourceType.MESSAGE_QUEUE,
        "aws.kinesis": MCCResourceType.MESSAGE_QUEUE,
        "kinesis": MCCResourceType.MESSAGE_QUEUE,
        "aws.kinesis-video": MCCResourceType.MESSAGE_QUEUE,
        "kinesis-video": MCCResourceType.MESSAGE_QUEUE,
        "aws.message-broker": MCCResourceType.MESSAGE_QUEUE,
        "aws.event-bus": MCCResourceType.EVENT_BUS,
        "aws.firehose": MCCResourceType.MESSAGE_QUEUE,
        
        # Auto Scaling
        "asg": MCCResourceType.AUTOSCALING_GROUP,
        "aws.asg": MCCResourceType.AUTOSCALING_GROUP,
        "launch-config": MCCResourceType.AUTOSCALING_GROUP,
        
        # Infrastructure as Code
        "aws.cfn": MCCResourceType.TERRAFORM_TEMPLATE,
        
        # Monitoring & Logging
        "log-group": MCCResourceType.LOG_GROUP,
        
        # Data & Analytics
        "aws.glue-catalog": MCCResourceType.DATABASE,
        "aws.glue-job": MCCResourceType.FUNCTION,
        "glue-job": MCCResourceType.FUNCTION,
        
        # Machine Learning
        "aws.sagemaker-notebook": MCCResourceType.INSTANCE,
        "sagemaker-notebook": MCCResourceType.INSTANCE,
        "sagemaker-endpoint-config": MCCResourceType.INSTANCE,
        "sagemaker-model": MCCResourceType.INSTANCE,
        
        # Developer Tools
        "aws.codebuild": MCCResourceType.INSTANCE,
        "codebuild": MCCResourceType.INSTANCE,
        "aws.codepipeline": MCCResourceType.INSTANCE,
        "aws.codedeploy-group": MCCResourceType.INSTANCE,
        
        # Application Services
        "aws.airflow": MCCResourceType.INSTANCE,
        "aws.app-flow": MCCResourceType.INSTANCE,
        "aws.step-machine": MCCResourceType.FUNCTION,
        
        # Management & Governance
        "aws.cloudtrail": MCCResourceType.LOG_GROUP,
        "aws.config-recorder": MCCResourceType.LOG_GROUP,
        "aws.dlm-policy": MCCResourceType.BACKUP,
        
        # Directory & Workspace
        "aws.directory": MCCResourceType.INSTANCE,
        "aws.workspaces-directory": MCCResourceType.INSTANCE,
        "aws.workspaces-image": MCCResourceType.IMAGE,
        
        # Routing
        "aws.route-table": MCCResourceType.NETWORK_GATEWAY,
        
        # Miscellaneous
        "ecs-task-definition": MCCResourceType.CONTAINER_SERVICE,
        "aws.ecs-task-definition": MCCResourceType.CONTAINER_SERVICE,
        "aws.codebuild-credential": MCCResourceType.SECRET,
        "aws.glue-security-configuration": MCCResourceType.SECURITY_GROUP,

        # Others
        "aws.log-group": MCCResourceType.LOG_GROUP,
        "aws.rest-resource": MCCResourceType.API_GATEWAY,
        "aws.rest-stage": MCCResourceType.API_GATEWAY,
        "aws.internet-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.rest-api": MCCResourceType.API_GATEWAY,
        "aws.sqs": MCCResourceType.MESSAGE_QUEUE,
        "aws.access-analyzer-finding": MCCResourceType.SECURITY_GROUP,
        "aws.alarm": MCCResourceType.LOG_GROUP,
        "aws.apigw-domain-name": MCCResourceType.API_GATEWAY,
        "aws.apigwv2": MCCResourceType.API_GATEWAY,
        "aws.appmesh-mesh": MCCResourceType.NETWORK_GATEWAY,
        "aws.appmesh-virtualgateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.appmesh-virtual-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.appmesh-virtualnode": MCCResourceType.NETWORK_GATEWAY,
        "aws.app-elb-target-group": MCCResourceType.LOAD_BALANCER,
        "aws.appstream-fleet": MCCResourceType.INSTANCE,
        "aws.appstream-stack": MCCResourceType.STACK,
        "aws.artifact-domain": MCCResourceType.CONTAINER_REGISTRY,
        "aws.artifact-repo": MCCResourceType.CONTAINER_REGISTRY,
        "aws.athena-named-query": MCCResourceType.DATABASE,
        "aws.batch-compute": MCCResourceType.INSTANCE,
        "aws.batch-definition": MCCResourceType.TEMPLATE,
        "aws.batch-queue": MCCResourceType.MESSAGE_QUEUE,
        "aws.bedrock-agent": MCCResourceType.FUNCTION,
        "aws.bedrock-custom-model": MCCResourceType.INSTANCE,
        "aws.bedrock-knowledge-base": MCCResourceType.DATABASE,
        "aws.bedrock-customization-job": MCCResourceType.FUNCTION,
        "aws.budget": MCCResourceType.CONFIG,
        "aws.cache-snapshot": MCCResourceType.SNAPSHOT,
        "aws.cache-subnet-group": MCCResourceType.SUBNET,
        "aws.cloudhsm-cluster": MCCResourceType.ENCRYPTION_KEY,
        "aws.cloudsearch": MCCResourceType.DATABASE,
        "aws.cloudwatch-dashboard": MCCResourceType.LOG_GROUP,
        "aws.codecommit": MCCResourceType.CONTAINER_REGISTRY,
        "aws.codedeploy-app": MCCResourceType.INSTANCE,
        "aws.codedeploy-deployment": MCCResourceType.INSTANCE,
        "aws.composite-alarm": MCCResourceType.LOG_GROUP,
        "aws.config-rule": MCCResourceType.CONFIG,
        "aws.connect-campaign": MCCResourceType.INSTANCE,
        "aws.connect-instance": MCCResourceType.INSTANCE,
        "aws.customer-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.datapipeline": MCCResourceType.MESSAGE_QUEUE,
        "aws.datasync-agent": MCCResourceType.INSTANCE,
        "aws.datasync-task": MCCResourceType.FUNCTION,
        "aws.dms-endpoint": MCCResourceType.NETWORK_GATEWAY,
        "aws.dms-replication-task": MCCResourceType.FUNCTION,
        "aws.dynamodb-backup": MCCResourceType.BACKUP,
        "aws.dynamodb-stream": MCCResourceType.MESSAGE_QUEUE,
        "aws.ec2-host": MCCResourceType.INSTANCE,
        "aws.ec2-spot-fleet-request": MCCResourceType.INSTANCE,
        "aws.ec2-capacity-reservation": MCCResourceType.INSTANCE,
        "aws.ecr-image": MCCResourceType.IMAGE,
        "aws.ecs-container-instance": MCCResourceType.CONTAINER_SERVICE,
        "aws.ecs-task": MCCResourceType.CONTAINER_SERVICE,
        "aws.efs-mount-target": MCCResourceType.FILE_SYSTEM,
        "aws.eks-nodegroup": MCCResourceType.K8S_CLUSTER,
        "aws.elasticbeanstalk": MCCResourceType.INSTANCE,
        "aws.emr-security-configuration": MCCResourceType.SECURITY_GROUP,
        "aws.emr-serverless-app": MCCResourceType.FUNCTION,
        "aws.event-rule": MCCResourceType.EVENT_BUS,
        "aws.event-rule-target": MCCResourceType.EVENT_BUS,
        "aws.firewall": MCCResourceType.SECURITY_GROUP,
        "aws.flow-log": MCCResourceType.LOG_GROUP,
        "aws.fsx-volume": MCCResourceType.VOLUME,
        "aws.gamelift-build": MCCResourceType.IMAGE,
        "aws.gamelift-fleet": MCCResourceType.AUTOSCALING_GROUP,
        "aws.glue-classifier": MCCResourceType.CONFIG,
        "aws.glue-connection": MCCResourceType.DATABASE,
        "aws.glue-crawler": MCCResourceType.FUNCTION,
        "aws.glue-database": MCCResourceType.DATABASE,
        "aws.glue-dev-endpoint": MCCResourceType.INSTANCE,
        "aws.glue-table": MCCResourceType.DATABASE,
        "aws.guardduty-finding": MCCResourceType.SECURITY_GROUP,
        "aws.healthcheck": MCCResourceType.LOG_GROUP,
        "aws.iam-oidc-provider": MCCResourceType.IAM_ROLE,
        "aws.iam-profile": MCCResourceType.IAM_ROLE,
        "aws.iam-saml-provider": MCCResourceType.IAM_ROLE,
        "aws.identity-pool": MCCResourceType.IAM_USER,
        "aws.inspector2-finding": MCCResourceType.SECURITY_GROUP,
        "aws.iot": MCCResourceType.INSTANCE,
        "aws.kinesis-analytics": MCCResourceType.MESSAGE_QUEUE,
        "aws.kinesis-analyticsv2": MCCResourceType.MESSAGE_QUEUE,
        "aws.kms": MCCResourceType.ENCRYPTION_KEY,
        "aws.lambda-layer": MCCResourceType.FUNCTION,
        "aws.launch-template-version": MCCResourceType.AUTOSCALING_GROUP,
        "aws.lightsail-db": MCCResourceType.DATABASE,
        "aws.lightsail-elb": MCCResourceType.LOAD_BALANCER,
        "aws.log-metric": MCCResourceType.LOG_GROUP,
        "aws.opensearch-serverless": MCCResourceType.DATABASE,
        "aws.org-account": MCCResourceType.TENANT,
        "aws.org-policy": MCCResourceType.IAM_POLICY,
        "aws.org-unit": MCCResourceType.TENANT,
        "aws.rds-cluster-param-group": MCCResourceType.CONFIG,
        "aws.rds-cluster-snapshot": MCCResourceType.SNAPSHOT,
        "aws.rds-param-group": MCCResourceType.CONFIG,
        "aws.rds-proxy": MCCResourceType.DATABASE,
        "aws.rds-subnet-group": MCCResourceType.SUBNET,
        "aws.rds-subscription": MCCResourceType.DATABASE,
        "aws.recovery-cluster": MCCResourceType.BACKUP,
        "aws.redshift": MCCResourceType.DATABASE,
        "aws.redshift-snapshot": MCCResourceType.SNAPSHOT,
        "aws.redshift-subnet-group": MCCResourceType.SUBNET,
        "aws.rest-account": MCCResourceType.TENANT,
        "aws.rest-client-certificate": MCCResourceType.CERTIFICATE,
        "aws.rest-vpclink": MCCResourceType.API_GATEWAY,
        "aws.s3-directory": MCCResourceType.BUCKET,
        "aws.s3-access-point": MCCResourceType.BUCKET,
        "aws.sagemaker-cluster": MCCResourceType.INSTANCE,
        "aws.sagemaker-domain": MCCResourceType.INSTANCE,
        "aws.sagemaker-endpoint": MCCResourceType.INSTANCE,
        "aws.sagemaker-job": MCCResourceType.FUNCTION,
        "aws.scaling-policy": MCCResourceType.AUTOSCALING_GROUP,
        "aws.securityhub-finding": MCCResourceType.SECURITY_GROUP,
        "aws.serverless-app": MCCResourceType.FUNCTION,
        "aws.service-quota": MCCResourceType.CONFIG,
        "aws.ses-configuration-set": MCCResourceType.CONFIG,
        "aws.ses-email-identity": MCCResourceType.CONFIG,
        "aws.shield-protection": MCCResourceType.SECURITY_GROUP,
        "aws.simpledb": MCCResourceType.DATABASE,
        "aws.snowball": MCCResourceType.INSTANCE,
        "aws.snowball-cluster": MCCResourceType.INSTANCE,
        "aws.sns-subscription": MCCResourceType.MESSAGE_QUEUE,
        "aws.ssm-activation": MCCResourceType.CONFIG,
        "aws.ssm-document": MCCResourceType.TEMPLATE,
        "aws.ssm-managed-instance": MCCResourceType.INSTANCE,
        "aws.ssm-parameter": MCCResourceType.CONFIG,
        "aws.storage-gateway": MCCResourceType.NETWORK_GATEWAY,
        "aws.streaming-distribution": MCCResourceType.CDN,
        "aws.timestream-database": MCCResourceType.DATABASE,
        "aws.timestream-table": MCCResourceType.DATABASE,
        "aws.transfer-server": MCCResourceType.NETWORK_GATEWAY,
        "aws.transfer-user": MCCResourceType.IAM_USER,
        "aws.user-pool": MCCResourceType.IAM_USER,
        "aws.wafv2": MCCResourceType.SECURITY_GROUP,
        "aws.workspaces-bundle": MCCResourceType.IMAGE,
    },
    Cloud.AZURE: {
        # Organization
        "azure.subscription": MCCResourceType.TENANT,
        "azure.resourcegroup": MCCResourceType.TENANT,
        
        # Compute
        "azure.vm": MCCResourceType.INSTANCE,
        "azure.vmss": MCCResourceType.AUTOSCALING_GROUP,
        "azure.batch": MCCResourceType.INSTANCE,
        "azure.webapp": MCCResourceType.INSTANCE,
        "azure.app-service-environment": MCCResourceType.INSTANCE,
        "azure.appserviceplan": MCCResourceType.INSTANCE,
        "azure.spring-service-instance": MCCResourceType.INSTANCE,
        
        # Containers
        "azure.container-registry": MCCResourceType.CONTAINER_REGISTRY,
        "azure.aks": MCCResourceType.K8S_CLUSTER,
        "azure.service-fabric-cluster": MCCResourceType.K8S_CLUSTER,
        
        # Storage
        "azure.disk": MCCResourceType.VOLUME,
        "azure.snapshot": MCCResourceType.SNAPSHOT,
        "azure.storage": MCCResourceType.BUCKET,
        "azure.recovery-services": MCCResourceType.BACKUP,
        
        # Database
        "azure.cosmosdb": MCCResourceType.DATABASE,
        "azure.sql-database": MCCResourceType.DATABASE,
        "azure.sql-server": MCCResourceType.DATABASE,
        "azure.mysql": MCCResourceType.DATABASE,
        "azure.mysql-flexibleserver": MCCResourceType.DATABASE,
        "azure.postgresql-server": MCCResourceType.DATABASE,
        "azure.postgresql-flexibleserver": MCCResourceType.DATABASE,
        "azure.mariadb-server": MCCResourceType.DATABASE,
        "azure.redis": MCCResourceType.CACHE,
        "azure.kusto": MCCResourceType.DATABASE,
        
        # Networking
        "azure.vnet": MCCResourceType.VPC,
        "azure.networksecuritygroup": MCCResourceType.SECURITY_GROUP,
        "azure.networkinterface": MCCResourceType.NETWORK_INTERFACE,
        "azure.publicip": MCCResourceType.IP_ADDRESS,
        "azure.loadbalancer": MCCResourceType.LOAD_BALANCER,
        "azure.application-gateway": MCCResourceType.LOAD_BALANCER,
        "azure.front-door": MCCResourceType.CDN,
        "azure.afd-custom-domain": MCCResourceType.CDN,
        "azure.waf": MCCResourceType.SECURITY_GROUP,
        
        # Security & Identity
        "azure.keyvault": MCCResourceType.SECRET,
        "azure.keyvault-key": MCCResourceType.ENCRYPTION_KEY,
        "azure.keyvault-secret": MCCResourceType.SECRET,
        "azure.roledefinition": MCCResourceType.IAM_POLICY,
        "azure.role": MCCResourceType.IAM_ROLE,
        
        # API & Integration
        "azure.api-management": MCCResourceType.API_GATEWAY,
        
        # Messaging & Events
        "azure.eventhub": MCCResourceType.MESSAGE_QUEUE,
        "azure.event-grid-domain": MCCResourceType.EVENT_BUS,
        "azure.event-grid-topic": MCCResourceType.EVENT_BUS,
        "azure.servicebus-namespace": MCCResourceType.MESSAGE_QUEUE,
        "azure.signalr": MCCResourceType.MESSAGE_QUEUE,
        
        # Monitoring & Logging
        "azure.monitor-log-profile": MCCResourceType.LOG_GROUP,
        "azure.alert-logs": MCCResourceType.LOG_GROUP,
        
        # Data & Analytics
        "azure.databricks": MCCResourceType.INSTANCE,
        "azure.datafactory": MCCResourceType.INSTANCE,
        "azure.stream-job": MCCResourceType.INSTANCE,
        "azure.synapse": MCCResourceType.DATABASE,
        
        # Machine Learning & AI
        "azure.machine-learning-workspace": MCCResourceType.INSTANCE,
        "azure.cognitiveservice": MCCResourceType.INSTANCE,
        
        # Application Services
        "azure.logic-app-workflow": MCCResourceType.FUNCTION,
        "azure.automation-account": MCCResourceType.INSTANCE,
        "azure.search": MCCResourceType.DATABASE,
        
        # IoT
        "azure.iothub": MCCResourceType.MESSAGE_QUEUE,
        
        # Security & Compliance
        "azure.defender-assessment": MCCResourceType.SECURITY_GROUP,
        "azure.defender-autoprovisioning": MCCResourceType.SECURITY_GROUP,
        "azure.defender-contact": MCCResourceType.SECURITY_GROUP,
        "azure.defender-pricing": MCCResourceType.SECURITY_GROUP,
        "azure.defender-setting": MCCResourceType.SECURITY_GROUP,
        
        # Configuration
        "azure.app-configuration": MCCResourceType.CONFIG,

        # Others
        "azure.advisor-recommendation": MCCResourceType.SECURITY_GROUP,
        "azure.app-insights": MCCResourceType.LOG_GROUP,
        "azure.open-shift": MCCResourceType.K8S_CLUSTER,
        "azure.bastion-host": MCCResourceType.NETWORK_GATEWAY,
        "azure.cdn-custom-domain": MCCResourceType.CDN,
        "azure.cdn-endpoint": MCCResourceType.CDN,
        "azure.cdnprofile": MCCResourceType.CDN,
        "azure.container-group": MCCResourceType.CONTAINER_SERVICE,
        "azure.containerregistry": MCCResourceType.CONTAINER_REGISTRY,
        "azure.containerservice": MCCResourceType.CONTAINER_SERVICE,
        "azure.cosmosdb-collection": MCCResourceType.DATABASE,
        "azure.cosmosdb-database": MCCResourceType.DATABASE,
        "azure.cost-management-export": MCCResourceType.CONFIG,
        "azure.datalake": MCCResourceType.BUCKET,
        "azure.datalake-analytics": MCCResourceType.INSTANCE,
        "azure.defender-alert": MCCResourceType.SECURITY_GROUP,
        "azure.defender-jit-policy": MCCResourceType.SECURITY_GROUP,
        "azure.dnszone": MCCResourceType.DNS_ZONE,
        "azure.eventsubscription": MCCResourceType.EVENT_BUS,
        "azure.front-door-policy": MCCResourceType.SECURITY_GROUP,
        "azure.hdinsight": MCCResourceType.INSTANCE,
        "azure.host-pool": MCCResourceType.INSTANCE,
        "azure.image": MCCResourceType.IMAGE,
        "azure.keyvault-certificate": MCCResourceType.CERTIFICATE,
        "azure.keyvault-keys": MCCResourceType.ENCRYPTION_KEY,
        "azure.mariadb": MCCResourceType.DATABASE,
        "azure.networkwatcher": MCCResourceType.LOG_GROUP,
        "azure.policyassignments": MCCResourceType.IAM_POLICY,
        "azure.postgresql-database": MCCResourceType.DATABASE,
        "azure.postgresql-flexibleserver": MCCResourceType.DATABASE,
        "azure.recordset": MCCResourceType.DNS_ZONE,
        "azure.roleassignment": MCCResourceType.IAM_ROLE,
        "azure.routetable": MCCResourceType.NETWORK_GATEWAY,
        "azure.servicebus-namespace-networkrules": MCCResourceType.SECURITY_GROUP,
        "azure.servicebus-namespace-authrules": MCCResourceType.IAM_POLICY,
        "azure.service-fabric-cluster-managed": MCCResourceType.K8S_CLUSTER,
        "azure.session-host": MCCResourceType.INSTANCE,
        "azure.spring-app": MCCResourceType.INSTANCE,
        "azure.sqldatabase": MCCResourceType.DATABASE,
        "azure.sqlserver": MCCResourceType.DATABASE,
        "azure.storage-container": MCCResourceType.BUCKET,
        "azure.traffic-manager-profile": MCCResourceType.LOAD_BALANCER,
        "azure.afd-custom-domain": MCCResourceType.CDN,
    },
    Cloud.GOOGLE: {
        # Organization
        "gcp.project": MCCResourceType.TENANT,
        "gcp.compute-project": MCCResourceType.TENANT,
        
        # Compute
        "gcp.instance": MCCResourceType.INSTANCE,
        "gcp.instance-group-manager": MCCResourceType.AUTOSCALING_GROUP,
        "gcp.instance-template": MCCResourceType.IMAGE,
        "gcp.image": MCCResourceType.IMAGE,
        "gcp.function": MCCResourceType.FUNCTION,
        "gcp.cloud-run-service": MCCResourceType.CONTAINER_SERVICE,
        "gcp.cloud-run-revision": MCCResourceType.CONTAINER_SERVICE,
        "gcp.app-engine": MCCResourceType.INSTANCE,
        
        # Containers
        "gcp.gke-cluster": MCCResourceType.K8S_CLUSTER,
        "gcp.gke-nodepool": MCCResourceType.AUTOSCALING_GROUP,
        
        # Storage
        "gcp.disk": MCCResourceType.VOLUME,
        "gcp.snapshot": MCCResourceType.SNAPSHOT,
        "gcp.bucket": MCCResourceType.BUCKET,
        
        # Database
        "gcp.sql-instance": MCCResourceType.DATABASE,
        "gcp.sql-user": MCCResourceType.IAM_USER,
        "gcp.spanner-instance": MCCResourceType.DATABASE,
        "gcp.spanner-database-instance": MCCResourceType.DATABASE,
        "gcp.spanner-backup": MCCResourceType.BACKUP,
        "gcp.bigtable-instance-cluster": MCCResourceType.DATABASE,
        "gcp.bigtable-instance-table": MCCResourceType.DATABASE,
        "gcp.bigtable-instance-cluster-backup": MCCResourceType.BACKUP,
        "gcp.bq-dataset": MCCResourceType.DATABASE,
        "gcp.bq-table": MCCResourceType.DATABASE,
        "gcp.bq-job": MCCResourceType.FUNCTION,
        "gcp.redis": MCCResourceType.CACHE,
        
        # Networking
        "gcp.vpc": MCCResourceType.VPC,
        "gcp.subnet": MCCResourceType.SUBNET,
        "gcp.firewall": MCCResourceType.SECURITY_GROUP,
        "gcp.loadbalancer-backend-service": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-target-https-proxy": MCCResourceType.LOAD_BALANCER,
        "gcp.armor-policy": MCCResourceType.SECURITY_GROUP,
        "gcp.dns-managed-zone": MCCResourceType.DNS_ZONE,
        
        # Security & Identity
        "gcp.kms-cryptokey": MCCResourceType.ENCRYPTION_KEY,
        "gcp.kms-keyring": MCCResourceType.ENCRYPTION_KEY,
        "gcp.kms-location": MCCResourceType.REGION,
        "gcp.secret": MCCResourceType.SECRET,
        "gcp.service-account-key": MCCResourceType.SECRET,
        "gcp.api-key": MCCResourceType.SECRET,
        "gcp.loadbalancer-ssl-certificate": MCCResourceType.CERTIFICATE,
        "gcp.loadbalancer-ssl-policy": MCCResourceType.SECURITY_GROUP,
        "gcp.app-engine-certificate": MCCResourceType.CERTIFICATE,
        "gcp.app-engine-firewall-ingress-rule": MCCResourceType.SECURITY_GROUP,
        "gcp.role": MCCResourceType.IAM_ROLE,
        
        # Messaging & Events
        "gcp.pubsub-topic": MCCResourceType.MESSAGE_QUEUE,
        "gcp.pubsub-subscription": MCCResourceType.MESSAGE_QUEUE,
        
        # Data & Analytics
        "gcp.dataflow-job": MCCResourceType.INSTANCE,
        "gcp.datafusion-instance": MCCResourceType.INSTANCE,
        "gcp.dataproc-clusters": MCCResourceType.INSTANCE,
        
        # Machine Learning
        "gcp.notebook": MCCResourceType.INSTANCE,
        
        # Management & Operations
        "gcp.patch-deployment": MCCResourceType.UNKNOWN,
        "gcp.service": MCCResourceType.UNKNOWN,

        # Others
        "gcp.autoscaler": MCCResourceType.AUTOSCALING_GROUP,
        "gcp.bigtable-instance": MCCResourceType.DATABASE,
        "gcp.build": MCCResourceType.FUNCTION,
        "gcp.cloudbilling-account": MCCResourceType.TENANT,
        "gcp.cloud-run-job": MCCResourceType.FUNCTION,
        "gcp.dm-deployment": MCCResourceType.STACK,
        "gcp.dns-policy": MCCResourceType.DNS_ZONE,
        "gcp.folder": MCCResourceType.TENANT,
        "gcp.iam-role": MCCResourceType.IAM_ROLE,
        "gcp.image": MCCResourceType.IMAGE,
        "gcp.interconnect": MCCResourceType.NETWORK_GATEWAY,
        "gcp.interconnect-attachment": MCCResourceType.NETWORK_GATEWAY,
        "gcp.kms-cryptokey-version": MCCResourceType.ENCRYPTION_KEY,
        "gcp.kms-keyring-iam-policy-bindings": MCCResourceType.IAM_POLICY,
        "gcp.loadbalancer-address": MCCResourceType.IP_ADDRESS,
        "gcp.loadbalancer-backend-bucket": MCCResourceType.BUCKET,
        "gcp.loadbalancer-forwarding-rule": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-global-address": MCCResourceType.IP_ADDRESS,
        "gcp.loadbalancer-global-forwarding-rule": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-health-check": MCCResourceType.LOG_GROUP,
        "gcp.loadbalancer-http-health-check": MCCResourceType.LOG_GROUP,
        "gcp.loadbalancer-https-health-check": MCCResourceType.LOG_GROUP,
        "gcp.loadbalancer-target-http-proxy": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-target-instance": MCCResourceType.INSTANCE,
        "gcp.loadbalancer-target-pool": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-target-ssl-proxy": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-target-tcp-proxy": MCCResourceType.LOAD_BALANCER,
        "gcp.loadbalancer-url-map": MCCResourceType.LOAD_BALANCER,
        "gcp.log-exclusion": MCCResourceType.LOG_GROUP,
        "gcp.log-project-metric": MCCResourceType.LOG_GROUP,
        "gcp.log-project-sink": MCCResourceType.LOG_GROUP,
        "gcp.ml-job": MCCResourceType.FUNCTION,
        "gcp.ml-model": MCCResourceType.INSTANCE,
        "gcp.organization": MCCResourceType.TENANT,
        "gcp.project-role": MCCResourceType.IAM_ROLE,
        "gcp.pubsub-snapshot": MCCResourceType.SNAPSHOT,
        "gcp.region": MCCResourceType.REGION,
        "gcp.role": MCCResourceType.IAM_ROLE,
        "gcp.route": MCCResourceType.NETWORK_GATEWAY,
        "gcp.router": MCCResourceType.NETWORK_GATEWAY,
        "gcp.service-account": MCCResourceType.IAM_ROLE,
        "gcp.sourcerepo": MCCResourceType.CONTAINER_REGISTRY,
        "gcp.sql-backup-run": MCCResourceType.SNAPSHOT,
        "gcp.sql-ssl-cert": MCCResourceType.CERTIFICATE,
        "gcp.zone": MCCResourceType.ZONE,
        "gcp.app-engine-domain": MCCResourceType.DNS_ZONE,
        "gcp.app-engine-domain-mapping": MCCResourceType.DNS_ZONE,
        "gcp.app-engine-service": MCCResourceType.CONTAINER_SERVICE,
        "gcp.app-engine-service-version": MCCResourceType.CONTAINER_SERVICE,
        "gcp.artifact-repository": MCCResourceType.CONTAINER_REGISTRY,
    },
    Cloud.K8S: _K8S_MAP,
    Cloud.KUBERNETES: _K8S_MAP,
}


