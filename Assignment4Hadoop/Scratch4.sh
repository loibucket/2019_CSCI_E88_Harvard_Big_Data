# wordcount
hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordcount s3://csci.e-88.principles.of.big.data.processing/input_files/ s3://csci.e-88.principles.of.big.data.processing/output_wordcount/

ssh -i BigDataHarvard.pem hadoop@ec2-52-15-222-73.us-east-2.compute.amazonaws.com

# aws s3 cp s3://BUCKETNAME/PATH/TO/FOLDER LocalFolderName --recursive

aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_wordcount  output_wordcount --recursive

aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query1  output_query1 --recursive
aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query2  output_query2 --recursive
aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query3  output_query3 --recursive
aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query4  output_query4 --recursive
aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query5  output_query5 --recursive

aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query1_r2  output_query1_r2 --recursive
aws s3 cp s3://csci.e-88.principles.of.big.data.processing/output_query1_r4  output_query1_r4 --recursive

# cat data | map | sort | reduce

cat ./input_files/* | ./hw4_problem2q1_mapper.py | sort | ./hw4_problem2q1_reducer.py > output_q1.txt

cat ./input_files/* | ./hw4_problem2q2_mapper.py | sort | ./hw4_problem2q2_reducer.py > output_q2.txt

cat ./input_files/* | ./hw4_problem2q3_mapper.py | sort | ./hw4_problem2q3_reducer.py > output_q3.txt

cat ./input_files/* | ./hw4_problem2q4_mapper.py | sort | ./hw4_problem2q4_reducer.py > output_q4.txt

cat ./input_files/* | ./hw4_problem2q5_mapper.py | sort | ./hw4_problem2q5_reducer.py > output_q5.txt

# master
ssh -i BigDataHarvard.pem hadoop@ec2-52-14-219-9.us-east-2.compute.amazonaws.com

# core
ssh -i BigDataHarvard.pem hadoop@ec2-3-15-150-151.us-east-2.compute.amazonaws.com

# core
ssh -i BigDataHarvard.pem hadoop@ec2-18-223-213-230.us-east-2.compute.amazonaws.com

# query 1
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./hw4_problem2q1_mapper.py -mapper ./hw4_problem2q1_mapper.py -file ./hw4_problem2q1_reducer.py -reducer ./hw4_problem2q1_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query1

# query 1 -D mapreduce.job.reduces=2
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -D mapreduce.job.reduces=2 -file ./hw4_problem2q1_mapper.py -mapper ./hw4_problem2q1_mapper.py -file ./hw4_problem2q1_reducer.py -reducer ./hw4_problem2q1_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query1_r2 

# query 1 -D mapreduce.job.reduces=4
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -D mapreduce.job.reduces=4 -file ./hw4_problem2q1_mapper.py -mapper ./hw4_problem2q1_mapper.py -file ./hw4_problem2q1_reducer.py -reducer ./hw4_problem2q1_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query1_r4 

# query 2
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./hw4_problem2q2_mapper.py -mapper ./hw4_problem2q2_mapper.py -file ./hw4_problem2q2_reducer.py -reducer ./hw4_problem2q2_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query2

# query 3
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./hw4_problem2q3_mapper.py -mapper ./hw4_problem2q3_mapper.py -file ./hw4_problem2q3_reducer.py -reducer ./hw4_problem2q3_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query3

# query 4
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./hw4_problem2q4_mapper.py -mapper ./hw4_problem2q4_mapper.py -file ./hw4_problem2q4_reducer.py -reducer ./hw4_problem2q4_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query4

# query 5
hadoop jar /usr/lib/hadoop/hadoop-streaming.jar -file ./hw4_problem2q5_mapper.py -mapper ./hw4_problem2q5_mapper.py -file ./hw4_problem2q5_reducer.py -reducer ./hw4_problem2q5_reducer.py -input s3://csci.e-88.principles.of.big.data.processing/input_files/ -output s3://csci.e-88.principles.of.big.data.processing/output_query5