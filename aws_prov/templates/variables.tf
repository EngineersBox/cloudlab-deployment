variable "private_key_name" {
	type    = "string"
  # Name of private key file to use on the AWS instances
	default = ""
}

variable "private_key_location" {
	type    = "string"
  # Path on your local machine to your SSH private key
	default = ""
}

variable "cluster_ami" {
  type = "string"
  # Base OS AMI to load onto the nodes
  default = "REQUIRED"
}

variable "aws_region" {
  type = "string"
  # Where should we provisioning the instances
  default = "us_east_1"
}

variable "instance_type" {
  type = "string"
  # What AWS instance size should we use (i.e. i8g.4xlarge)
  default = "t3.small"
}
