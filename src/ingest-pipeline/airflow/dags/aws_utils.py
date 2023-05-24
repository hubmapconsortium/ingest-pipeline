from operator import itemgetter

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


def _get_latest_ami_id(image_prefix):
    """Returns the AMI ID of the most recently-created Amazon Linux image"""

    # Amazon is retiring AL2 in 2023 and replacing it with Amazon Linux 2022.
    # This image prefix should be futureproof, but may need adjusting depending
    # on how they name the new images.  This page should have AL2022 info when
    # it comes available: https://aws.amazon.com/linux/amazon-linux-2022/faqs/
    client = AwsBaseHook(client_type="ec2").get_conn()
    images = client.describe_images(
        Filters=[{"Name": "tag:Name", "Values": [image_prefix]}]
    )
    # Sort on CreationDate
    sorted_images = sorted(images["Images"], key=itemgetter("CreationDate"), reverse=True)
    return sorted_images[0]["ImageId"]


def create_key_pair(key_name: str):
    client = AwsBaseHook(client_type="ec2").get_conn()

    try:
        key_pair_id = client.create_key_pair(KeyName=key_name)["KeyName"]
    except:
        key_pair_id = key_name
        print(f'Was the {key_name} key before the call?')
    # Creating the key takes a very short but measurable time, preventing race condition:
    client.get_waiter("key_pair_exists").wait(KeyNames=[key_pair_id])

    return key_pair_id


def create_instance(instance_name: str, instance_prefix: str, instance_type: str):
    client = AwsBaseHook(client_type="ec2").get_conn()

    # Create the instance
    instance_id = client.run_instances(
        ImageId=_get_latest_ami_id(instance_prefix),
        MinCount=1,
        MaxCount=1,
        InstanceType=instance_type,
        KeyName=create_key_pair(f"{instance_name}_keys"),
        TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": instance_name}]}],
        # Use IMDSv2 for greater security, see the following doc for more details:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
        MetadataOptions={"HttpEndpoint": "enabled", "HttpTokens": "required"},
        SecurityGroupIds=['sg-0907a55bc38412a01'],
    )["Instances"][0]["InstanceId"]

    # Wait for it to exist
    waiter = client.get_waiter("instance_status_ok")
    waiter.wait(InstanceIds=[instance_id])

    return instance_id


def terminate_instance(instance: str, instance_name: str):
    AwsBaseHook(client_type="ec2").get_conn().delete_key_pair(KeyName=f"{instance_name}_keys")
    AwsBaseHook(client_type="ec2").get_conn().terminate_instances(InstanceIds=[instance])
