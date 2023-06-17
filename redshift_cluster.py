# Create Redshift Cluster

# import libraries
import boto3

# don't forget to set AWS config file


# # Set up Redshift Client
redshift = boto3.client('redshift', aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key,
                               region_name='us-east-1')

# # Set the necessary info for creation
cluster_name = 'desired_cluster_name',    # str; suggestion: 'ds4a-cluster'
username = 'desired_username',            # str; suggestion: 'dsa4-team14-redhisft'
password = 'desired_password',        
IamRoles = '<iam_arn>',                   # copy ARN from IAM role    
node_type = 'dc2.large'                   # see list of available redshift node type options
database_name = 'dev',                    # e.g. dev, prod, etc  



# # Create cluster
try:
    response = redshift.create_cluster(
        ClusterType = 'single-node',                # str, specify node type (single-node or multi-node)
        ClusterIdentifier = 'desired-cluster-iden', # (str, ASCII letters, digits, hyphen ) suggestion: 'ds4a-cluster-main'
        NumberOfNodes = int(1),                     # specify desired number of nodes
        DBName = 'database_name',                   # str
        MasterUsername = 'desired_db_username',     # str
        MasterUserPassword = 'desired_db_pw',       # str, uppercase, lowercase, #, len(8), 
        IamRoles = IamRoles,
        NodeType = node_type,
    )
except Exception as e:
    print(e)