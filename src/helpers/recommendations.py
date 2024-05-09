RULE_RECOMMENDATION_MAPPING = {
    "ecc-aws-027-prevent_0-65535_ingress_and_all": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "ALL"
            ]
        }
    },
    "ecc-aws-028-security_group_ingress_is_restricted_traffic_to_dns_port_53": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "53"
            ]
        }
    },
    "ecc-aws-030-security_group_ingress_is_restricted_traffic_to_http_port_80": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "80"
            ]
        }
    },
    "ecc-aws-032-security_group_ingress_is_restricted_traffic_to_mongodb_port_27017": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "27017"
            ]
        }
    },
    "ecc-aws-035-security_group_ingress_is_restricted_traffic_to_oracle_db_port_1521": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "1521"
            ]
        }
    },
    "ecc-aws-036-security_group_ingress_is_restricted_traffic_to_pop3_port_110": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "110"
            ]
        }
    },
    "ecc-aws-039-security_group_ingress_is_restricted_traffic_to_telnet_port_23": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "23"
            ]
        }
    },
    "ecc-aws-062-security_group_ingress_is_restricted_22": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "22"
            ]
        }
    },
    "ecc-aws-063-security_group_ingress_is_restricted_3389": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "3389"
            ]
        }
    },
    "ecc-aws-131-instance_with_unencrypted_service_is_exposed_to_public_internet": {
        "resource_id": "{InstanceId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "9200",
                "9300",
                "11211",
                "27017",
                "61620",
                "9090",
                "22",
                "389",
                "1521",
                "2483",
                "6379",
                "7000",
                "7199",
                "8888",
                "9042",
                "9160",
                "3389"
            ]
        }
    },
    "ecc-aws-132-public_instance_with_sensitive_service_is_exposed_to_entire_internet": {
        "resource_id": "{InstanceId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                135,
                636,
                1433,
                2383,
                2484,
                3306,
                5432,
                7001,
                9000,
                11214,
                11215,
                23,
                445,
                25,
                110,
                137,
                138,
                139,
                161,
                53,
                3000,
                3020,
                4505,
                4506,
                8000,
                8080,
                5500,
                5900,
                1434,
                2382,
                8140,
                27018,
                61621
            ]
        }
    },
    "ecc-aws-151-security_group_ingress_is_restricted_traffic_to_port_20": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "20"
            ]
        }
    },
    "ecc-aws-167-security_group_ingress_is_restricted_traffic_to_port_143": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "143"
            ]
        }
    },
    "ecc-aws-168-security_group_ingress_is_restricted_traffic_to_mssql_ports": {
        "resource_id": "{GroupId}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "1433",
                "1434"
            ]
        }
    },
    "ecc-aws-186-ec2_instance_no_public_ip": {
        "resource_id": "{InstanceId}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "MAKE_IP_PRIVATE"
        }
    },

    "ecc-gcp-031-no_rdp_from_internet": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "3389"
            ]
        }
    },
    "ecc-gcp-033-vpc_flow_logs_for_every_subnet": {
        "resource_id": "{selfLink}",
        "resource_type": "SUBNET",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SUBNET"],
        "recommendation": {
            "action": "ENABLE_FLOW_LOGS"
        }
    },
    "ecc-gcp-032-private_google_access_for_all_subnetworks": {
        "resource_id": "{selfLink}",
        "resource_type": "SUBNET",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SUBNET"],
        "recommendation": {
            "action": "ENABLE_PRIVATE_GOOGLE_ACCESS"
        }
    },
    "ecc-gcp-034-no_instances_default_service_account_with_full_cloud_api_access": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "RESTRICT_FULL_ACCESS"
        }
    },
    "ecc-gcp-035-block_project-wide_ssh_keys_for_instances": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "BLOCK_PROJECT_WIDE_SSH"
        }
    },
    "ecc-gcp-038-not_ip_forwarding_on_instance": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "DISABLE_IP_FORWARDING"
        }
    },
    "ecc-gcp-071-inbound_traffic_restricted_to_that_which_is_necessary": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "ALL"
            ]
        }
    },
    "ecc-gcp-072-outbound_traffic_restricted_to_that_which_is_necessary": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_OUTBOUND",
            "port": [
                "ALL"
            ]
        }
    },
    "ecc-gcp-104-default_firewall_rule_in_use": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "action": "REMOVE_DEFAULT_FIREWALL",
            "port": []
        }
    },
    "ecc-gcp-109-prevent_allow_all_ingress": {
            "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "ALL"
            ]
        }
    },
    "ecc-gcp-110-firewall_allows_internet_traffic_to_dns_port_53": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "53"
            ]
        }
    },
    "ecc-gcp-111-firewal_allows_internet_traffic_to_ftp_port_21": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "21"
            ]
        }
    },
    "ecc-gcp-112-firewall_allows_internet_traffic_to_http": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "80"
            ]
        }
    },
    "ecc-gcp-113-firewall_allows_internet_traffic_microsoft-ds": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "445"
            ]
        }
    },
    "ecc-gcp-114-firewall_allows_internet_traffic_to_mongodb": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "27017"
            ]
        }
    },
    "ecc-gcp-115-firewall_allows_internet_traffic_to_mysql_db": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "3306"
            ]
        }
    },
    "ecc-gcp-116-firewall_allows_internet_traffic_to_netbios-ssn": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "139"
            ]
        }
    },
    "ecc-gcp-117-firewall_allows_internet_traffic_to_oracle_db": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "1521 "
            ]
        }
    },
    "ecc-gcp-118-firewal_allows_internet_traffic_to_pop3": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "110"
            ]
        }
    },
    "ecc-gcp-119-firewall_allows_internet_traffic_to_postgresql": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "5432"
            ]
        }
    },
    "ecc-gcp-120-firewall_allows_internet_traffic_to_smtp": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "25"
            ]
        }
    },
    "ecc-gcp-121-firewall_allows_internet_traffic_to_telnet": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "23"
            ]
        }
    },
    "ecc-gcp-151-vm_instances_enabled_with_pre-emptible_termination": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "DISABLE_PRE_EMPTIBLE_TERMINATION"
        }
    },
    "ecc-gcp-171-not_default_service_account_on_instance": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "REMOVE_DEFAULT_SERVICE_ACCOUNT"
        }
    },
    "ecc-gcp-173-instance_do_not_have_public_ip": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "MAKE_IP_PRIVATE"
        }
    },
    "ecc-gcp-194-oslogin_disabled_for_instance": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "ENABLE_OS_LOGIN"
        }
    },
    "ecc-gcp-195-clouddns_logging_is_enabled_for_all_vpc_networks": {
        "resource_id": "{gcp.vpc}",
        "resource_type": "VPC",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["VPC"],
        "recommendation": {
            "action": "ENABLE_DNS_LOGGING"
        }
    },
    "ecc-gcp-228-on_host_maintenance_set_to_migrate_for_instance": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "ENABLE_LIVE_MIGRATION"
        }
    },
    "ecc-gcp-232-instance_configured_with_enable_oslogin_2fa": {
        "resource_id": "{selfLink}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "ENABLE_OF_LOGIN_2FA"
        }
    },
    "ecc-gcp-278-firewall_allows_internet_traffic_to_elastic_search": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "9200",
                "9300"
            ]
        }
    },
    "ecc-gcp-279-firewall_allows_internet_traffic_to_ftp_port_20": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "20"
            ]
        }
    },
    "ecc-gcp-280-firewall_allows_internet_traffic_to_kibana": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "5601"
            ]
        }
    },
    "ecc-gcp-283-firewall_allows_internet_traffic_to_sql_server": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "1433"
            ]
        }
    },
    "ecc-gcp-285-firewall_allows_internet_traffic_to_winrm": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "5985",
                "5986"
            ]
        }
    },
    "ecc-gcp-288-firewall_allow_unrestricted_inbound_access_using_icmp": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "icmp",
            "action": "RESTRICT_INBOUND",
            "port": []
        }
    },
    "ecc-gcp-289-firewall_allow_unrestricted_inbound_access_using_rpc": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "135"
            ]
        }
    },
    "ecc-gcp-342-firewall_allows_internet_traffic_Hadoop-HDFS": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "8020"
            ]
        }
    },
    "ecc-gcp-337-firewall_allows_internet_traffic_VNC-Server": {
        "resource_id": "{selfLink}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "5900"
            ]
        }
    },

    "ecc-azure-113-cis_vm_utilizing_managed_disks": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "UNMANAGED_DISKS"
        }
    },
    "ecc-azure-116-cis_vm_endpoint_protection": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "CONFIGURE_ENDPOINT_PROTECTION"
        }
    },
    "ecc-azure-184-asb_vm_linux_ssh_auth_req": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "CONFIGURE_SSH_AUTHENTICATION"
        }
    },
    "ecc-azure-197-asb_vm_disk_encryption_on": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "ENABLE_DISC_ENCRYPTION"
        }
    },
    "ecc-azure-231-asb_vm_wo_mma": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "CONFIGURE_LOG_ANALYTICS_AGENT"
        }
    },
    "ecc-azure-275-asb_vm_backup": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "CONFIGURE_BACKUP"
        }
    },
    "ecc-azure-367-vm_omi_vulnerability": {
        "resource_id": "{id}",
        "resource_type": "INSTANCE",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["INSTANCE"],
        "recommendation": {
            "action": "PROTECT_FROM_OMI_VULNERABILITY"
        }
    },
    "ecc-azure-048-cis_net_rdp": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "3389"
            ]
        }
    },
    "ecc-azure-049-cis_net_ssh": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "22"
            ]
        }
    },
    "ecc-azure-052-cis_net_udp": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "udp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "ALL"
            ]
        }
    },
    "ecc-azure-119-nsg_all": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "ALL"
            ]
        }
    },
    "ecc-azure-120-nsg_dns": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "53"
            ]
        }
    },
    "ecc-azure-121-nsg_ftp": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "21"
            ]
        }
    },
    "ecc-azure-122-cis_nsg_http": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "80"
            ]
        }
    },
    "ecc-azure-123-nsg_microsoft_ds": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "445"
            ]
        }
    },
    "ecc-azure-124-nsg_mongo_db": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "27017"
            ]
        }
    },
    "ecc-azure-125-nsg_mysql": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "3306"
            ]
        }
    },
    "ecc-azure-126-nsg_netbios": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "139"
            ]
        }
    },
    "ecc-azure-127-nsg_oracle_db": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "1521"
            ]
        }
    },
    "ecc-azure-128-nsg_pop3": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "110"
            ]
        }
    },
    "ecc-azure-129-nsg_postgresql": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "5432"
            ]
        }
    },
    "ecc-azure-130-nsg_smtp": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "25"
            ]
        }
    },
    "ecc-azure-131-nsg_telnet": {
        "resource_id": "{id}",
        "resource_type": "SECURITY_GROUP",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": ["SECURITY_GROUP"],
        "recommendation": {
            "protocol": "tcp",
            "action": "RESTRICT_INBOUND",
            "port": [
                "23"
            ]
        }
    }
}

K8S_RECOMMENDATION_MODEL = {
        "resource_id": "{cluster_id}",
        "resource_type": "K8S_CLUSTER",
        "source": "CUSTODIAN",
        "severity": "HIGH",
        "stats": {
            "scan_date": None,
            "status": "OK",
            "message": "Processed successfully"
        },
        "meta": None,
        "general_actions": [],  # ROLE, POD CONFIG
        "recommendation": {
            "resource_id": "{id}",
            "resource_type": "{type}",
            "article": "{article}",
            "impact": "{impact}",
            "description": "{description}"
        }
    }
