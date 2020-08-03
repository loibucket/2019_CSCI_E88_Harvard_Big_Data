#master
scp -i BigDataHarvard.pem *.py hadoop@ec2-18-225-35-225.us-east-2.compute.amazonaws.com:.

#scp -i BigDataHarvard.pem *.jar hadoop@ec2-52-14-131-43.us-east-2.compute.amazonaws.com:.
#scp -i BigDataHarvard.pem hadoop@ec2-52-14-131-43.us-east-2.compute.amazonaws.com:./spark*.csv ./ 