scp your_username@remotehost.edu:foobar.txt /some/local/directory

ssh -i "BigDataHarvard.pem" centos@ec2-3-14-10-185.us-east-2.compute.amazonaws.com

scp -i "BigDataHarvard.pem" -r centos@ec2-3-14-10-185.us-east-2.compute.amazonaws.com:input_files .

wget https://repo.anaconda.com/archive/Anaconda3-2019.07-Linux-x86_64.sh

bash Anaconda3-2019.07-Linux-x86_64.sh


----------

ssh -i "BigDataHarvard.pem" centos@ec2-18-216-234-236.us-east-2.compute.amazonaws.com

scp -i "BigDataHarvard.pem" hw2_problem1_IntenseFibonacci.py centos@ec2-18-216-234-236.us-east-2.compute.amazonaws.com:/home/centos

alias python='/usr/bin/python3.6'

the name of the AMI is “Harvard-e88-HW2-2019-new”

ssh -i "BigDataHarvard.pem" centos@ec2-3-19-238-105.us-east-2.compute.amazonaws.com

scp -i "BigDataHarvard.pem" hw2_problem1_IntenseFibonacci.py centos@ec2-3-19-238-105.us-east-2.compute.amazonaws.com:/home/centos

----------

ssh -i "BigDataHarvard.pem" centos@ec2-18-188-248-141.us-east-2.compute.amazonaws.com

scp -i "BigDataHarvard.pem" hw2_problem2_IntenseIO.py centos@ec2-18-188-248-141.us-east-2.compute.amazonaws.com:/home/centos

----------

ssh -i "BigDataHarvard.pem" centos@ec2-18-219-7-13.us-east-2.compute.amazonaws.com

scp -i "BigDataHarvard.pem" hw2_problem2_IntenseIO.py centos@ec2-18-219-7-13.us-east-2.compute.amazonaws.com:/home/centos
