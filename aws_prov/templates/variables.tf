variable "private_key_name" {
	type    = "string"
	default = ""
}

variable "private_key_location" {
	type    = "string"
	default = ""
}

variable "cluster_ami" {
  type = "string"
  default = "REQUIRED"
}

variable "aws_region" {
  type = "string"
  default = "us_east_1"
}
