ssh -i "bigdataHarvard.pem" centos@ec2-3-14-79-190.us-east-2.compute.amazonaws.com

scp -i "bigdataHarvard.pem" hw1_problem1.py centos@ec2-3-14-79-190.us-east-2.compute.amazonaws.com:/home/centos

docker run --name rediserve -d redis

docker run --name client1 -d redis

docker run --name client2 -d redis

docker exec -it client1 bash

docker exec -it client2 bash

redis-cli -p 6380

docker run -p 5432:5432 --name pgdemo -e POSTGRES_PASSWORD=1234 -d postgres

hostname -I