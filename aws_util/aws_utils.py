import boto3
import configparser
import json
import pandas as pd

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

iam = boto3.client("iam", aws_access_key_id=config["AWS"]["key"],
                   aws_secret_access_key=config["AWS"]["secret"],
                   region_name=config["AWS"]["region"]
                   )
redshift = boto3.client("redshift",
                        region_name=config["AWS"]["region"],
                        aws_access_key_id=config["AWS"]["key"],
                        aws_secret_access_key=config["AWS"]["secret"]
                        )
ec2 = boto3.resource("ec2",
                     region_name=config["AWS"]["region"],
                     aws_access_key_id=config["AWS"]["key"],
                     aws_secret_access_key=config["AWS"]["secret"]
                     )

s3 = boto3.resource('s3',
                    region_name=config["AWS"]["region"],
                    aws_access_key_id=config["AWS"]["key"],
                    aws_secret_access_key=config["AWS"]["secret"]
                    )


def __update_param(section, param, value):
    """Update configParser() param values and writes to dwg.cfg
       args: section, param and value to be updated"""
    try:
        config.set(section, param, value)
        with open('dwh.cfg', "w") as f:
            config.write(f)
    except Exception as e:
        print(e)


def create_dwh_role():
    """Connects to Amazon Web Services and creates roles based on dwh.cfg parameters.
    updates config and dwg.cfg with updated role arn."""
    try:
        iam.create_role(
            Path="/",
            RoleName=config["IAM_ROLE"]["iam_role_name"],
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {"Statement": [{"Action": "sts:AssumeRole",
                                "Effect": "Allow",
                                "Principal": {"Service": "redshift.amazonaws.com"}}],
                 "Version": "2012-10-17"})
        )
        iam.attach_role_policy(RoleName=config["IAM_ROLE"]["iam_role_name"],
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                               )["ResponseMetadata"]["HTTPStatusCode"]
    except Exception as e:
        print(e)
    role_arn = iam.get_role(RoleName=config["IAM_ROLE"]["iam_role_name"])["Role"]["Arn"]
    __update_param("IAM_ROLE", "ARN", role_arn)


def create_cluster():
    """"Connects to Amazon Web Services and creates cluster based on dwh.cfg parameters."""
    try:
        redshift.create_cluster(
            # HW
            ClusterType=config["CLUSTER"]["cluster_type"],
            NodeType=config["CLUSTER"]["node_type"],
            # NumberOfNodes=int(config["CLUSTER"]["num_nodes"]),

            # Identifiers & Credentials
            DBName=config["CLUSTER"]["db_name"],
            ClusterIdentifier=config["CLUSTER"]["cluster_name"],
            MasterUsername=config["CLUSTER"]["db_user"],
            MasterUserPassword=config["CLUSTER"]["db_password"],

            # Roles (for s3 access)
            IamRoles=[config["IAM_ROLE"]["arn"]]
        )

    except Exception as e:
        print(e)


def create_vpc_rule():
    """"Connects to Amazon Web Services and creates cluster based on dwh.cfg parameters."""
    try:
        my_cluster_props = redshift.describe_clusters(ClusterIdentifier=config["CLUSTER"]["cluster_name"])["Clusters"][
            0]
        vpc = ec2.Vpc(id=my_cluster_props["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(GroupName=defaultSg.group_name,
                                    CidrIp="0.0.0.0/0",
                                    IpProtocol="TCP",
                                    FromPort=int(config["CLUSTER"]["db_port"]),
                                    ToPort=int(config["CLUSTER"]["db_port"])
                                    )
    except Exception as e:
        print(e)


def __prettyRedshiftProps(props):
    pd.set_option("display.max_colwidth", None)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                  "NumberOfNodes", "VpcId"]
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def query_cluster():
    """"Returns cluster status."""
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config["CLUSTER"]["cluster_name"])["Clusters"][0]
    print(__prettyRedshiftProps(myClusterProps))


def get_conn_string():
    """"Returns formatted connection string to cluster database."""
    endpoint = redshift.describe_clusters(ClusterIdentifier=config["CLUSTER"]["cluster_name"])["Clusters"][0]["Endpoint"][
        "Address"]
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(config["CLUSTER"]["db_user"], config["CLUSTER"]["db_password"],
                                                       endpoint, config["CLUSTER"]["db_port"],
                                                       config["CLUSTER"]["db_name"])
    return conn_string


def drop_cluster():
    """"Connects to Amazon Web Services and deletes cluster based on dwh.cfg parameters."""
    redshift.delete_cluster(ClusterIdentifier=config["CLUSTER"]["cluster_name"], SkipFinalClusterSnapshot=True)


def drop_iam_role():
    """"Connects to Amazon Web Services and deletes cluster based on dwh.cfg parameters."""
    iam.detach_role_policy(RoleName=config["IAM_ROLE"]["iam_role_name"],
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=config["IAM_ROLE"]["iam_role_name"])


def get_s3_client():
    """"Returns amazon S3 client based on dwh.cfg access parameters."""
    return s3


def main():
    create_dwh_role()
    create_cluster()
    create_vpc_rule()


if __name__ == "__main__":
    main()
