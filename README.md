# cloudlab-deployment

GENI Profile for deploying DBs with a dedicated OTEL collector in CloudLab or on AWS

## Setup 

First initialise the uv project via:

```bash
./init.sh
```

## CloudLab Usage

Generate run the profile with, supply `-h/--help` to see all options:

```bash
uv run profile.py [...arguments]
```

Use the generated `profile.xml` file in a profile on CloudLab for provisioning.

## AWS Usage

First, generate the CloudLab profile in the above steps, then [install the terraform CLI](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

Then convert the profile to terraform code for AWS usage:

```bash
uv run aws_prov/main.py <path/to/profile.xml> <application type> <output directory path>
```

Then go to the output directory.

Fill in the `variables.tf` file

run the following to deploy the infrastructure

```bash
terraform init
terraform plan -out=tf.plan
```

Verify that there are no mistakes or misconfigurations in the plan,
then once you are comfortable, apply the plan

```bash
terraform apply tf.plan
```

This will output the public IPs for each node in the cluster, you
can SSH to them and perform the necessary experiments.

Once complete, to destroy the deployment simply run

```bash
terraform destroy
```
