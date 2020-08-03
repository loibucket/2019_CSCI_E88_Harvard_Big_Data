python hw3_problem1.py "input_files/file-input1.csv"

get "2019-09-13:01:http://example.com/?url=035:unique_user_count"
get "2019-09-14:10:http://example.com/?url=043:unique_user_count"

get "2019-09-14:14:http://example.com/?url=042:event_count"
get "2019-09-12:19:http://example.com/?url=013:event_count"
get "2019-09-14:03:http://example.com/?url=162:event_count"
get "2019-09-13:01:http://example.com/?url=035:event_count"
get "2019-09-14:10:http://example.com/?url=043:event_count"

get "2019-09-14:02,BW:unique_url_count"
get "2019-09-13:05,KI:unique_url_count"
get "2019-09-13:08,DJ:unique_url_count"
get "2019-09-13:16,AS:unique_url_count"
get "2019-09-14:00,VE:unique_url_count"

docker-compose -f hw3_redis_docker-compose.yml up
docker-compose -f hw3_p1_docker-compose.yml up
docker-compose -f hw3_p2_docker-compose.yml up
docker-compose -f hw3_p3_docker-compose.yml up

ZRANGE "2019-09-12:Average_TTFB" 0 4 WITHSCORES

ssh -i BigDataHarvard.pem hadoop@ec2-18-222-226-109.us-east-2.compute.amazonaws.com