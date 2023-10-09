project_id = "my-capstone-project-401111"
region     = "us-central1"
location     = "us-central1-a"

#GKE
gke_num_nodes = 2
machine_type  = "n1-standard-1"

#CloudSQL
instance_name     = "capstone-project-01"
database_version  = "POSTGRES_12"
instance_tier     = "db-f1-micro"
disk_space        = 10
database_name     = "dbcapstone"
db_username       = "dbcapstone_user"
db_password       = "dbcapstone@taiwo"
