import pandas as pd
import boto3
import json
import configparser


def get_config():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    return config


def get_role_arn(config):
    iam = iam_client(config)

    try:
        print('Creating a new IAM Role')
        iam.create_role(
            Path='/',
            RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
            Description='Allows Redshift cluster to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {
                    'Statement': [{
                        'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}
                    }],
                    'Version': '2012-10-17'
                }
            )
        )

        print('Attaching Policy')

        attaching_policy_result = iam.attach_role_policy(
            RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        print(attaching_policy_result)

        print('Get the IAM role ARN')
        role_arn = iam_role(config, iam)

        print(role_arn['Role']['Arn'])
    except Exception as e:
        print(e)

    return iam_role(config, iam)


def iam_role(config, iam):
    return iam.get_role(
        RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
    )


def iam_client(config):
    iam = boto3.client('iam',
                       region_name='us-east-2',
                       aws_access_key_id=config.get('AWS', 'KEY'),
                       aws_secret_access_key=config.get('AWS', 'SECRET'))
    return iam


def redshift_client(config):
    return boto3.client('redshift',
                        region_name='us-east-2',
                        aws_access_key_id=config.get('AWS', 'KEY'),
                        aws_secret_access_key=config.get('AWS', 'SECRET'))


def redshift_cluster(role_arn, config):
    redshift = redshift_client(config)
    try:
        print([role_arn['Role']['Arn']])
        response = redshift.create_cluster(
            # add parameters for hardware
            ClusterType=config.get("DWH", "DWH_CLUSTER_TYPE"),
            NodeType=config.get("DWH", "DWH_NODE_TYPE"),
            NumberOfNodes=int(config.get("DWH", "DWH_NUM_NODES")),
            # add parameters for identifiers & credentials
            DBName=config.get("DWH", "DWH_DB"),
            ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
            MasterUsername=config.get("DWH", "DWH_DB_USER"),
            MasterUserPassword=config.get("DWH", "DWH_DB_PASSWORD"),
            # add parameter for role (to allow s3 access)
            IamRoles=[role_arn['Role']['Arn']]
        )
        print(response['ResponseMetadata']['HTTPStatusCode'])
    except Exception as e:
        print(e)


def pretty_redshift_properties(props):
    pd.set_option('display.max_colwidth', -1)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                    "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    print(pd.DataFrame(data=x, columns=["Key", "Value"]))


def cluster_status():
    pretty_redshift_properties(cluster_properties())


def cluster_properties():
    config = get_config()
    redshift = redshift_client(config)
    return redshift.describe_clusters(ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]


def create_cluster():
    config = get_config()
    role = get_role_arn(config)
    redshift_cluster(role, config)
    cluster_status()


def create_vpc():
    config = get_config()
    try:
        ec2 = get_ec2_client()
        print(cluster_properties()['VpcId'])
        vpc = ec2.Vpc(id=cluster_properties()['VpcId'])
        default_strategy = list(vpc.security_groups.all())[0]
        print(default_strategy.group_name)
        print(default_strategy)

        default_strategy.authorize_ingress(
            GroupName=default_strategy.group_name,  # TODO: fill out
            CidrIp='0.0.0.0/0',  # TODO: fill out
            IpProtocol='TCP',  # TODO: fill out
            FromPort=int(config.get("DWH", "DWH_PORT"), ),
            ToPort=int(config.get("DWH", "DWH_PORT"), )
        )
    except Exception as e:
        print(e)


def get_ec2_client():
    config = get_config()
    ec2 = boto3.resource('ec2',
                         region_name='us-east-2',
                         aws_access_key_id=config.get('AWS', 'KEY'),
                         aws_secret_access_key=config.get('AWS', 'SECRET'))
    return ec2


def delete_cluster():
    config = get_config()
    redshift = redshift_client(config)
    print("Deleting cluster...")
    redshift.delete_cluster(ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
                            SkipFinalClusterSnapshot=True)

    iam = iam_client(config)
    print("Detaching policy...")
    iam.detach_role_policy(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    print("Deleting role...")
    iam.delete_role(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"), )


def show_cluster_info():
    config = get_config()
    iam = iam_client(config)
    role = iam_role(config, iam)

    print('----------------------------------\n')
    print('ARN Role: '+ role['Role']['Arn'])

    properties = cluster_properties()
    pretty_redshift_properties(properties)


def help():
    print("1. create_cluster()")
    print("2. cluster_status()")
    print("3. create_vpc()")
    print("4. show_cluster_info()")
    print("4. delete_cluster()")
