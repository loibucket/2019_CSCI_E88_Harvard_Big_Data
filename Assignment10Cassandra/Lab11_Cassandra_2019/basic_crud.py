from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra import util

# Initialize the connection and session with Cassandra on localhost
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('lab10')

def insert_cassandra(sensor_type, time_hour, reading_time, sensor_id, metric):
    session.execute(
        """



        """
    print ("inserted event: ", sensor_type, time_hour, reading_time, sensor_id, metric)


def main():
    event_count = 2
    base_metric_value = 1.1
    current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    two_min_time_delta = timedelta(minutes=2)
    types = ["p_type1", "p_type2"]
    for i in range(event_count):
        for st in types:
            reading_time = current_hour + i*two_min_time_delta
            metric_value = i * base_metric_value
            ## generate time-based uuid using Cassandra driver's util
            sensor_id = util.min_uuid_from_time(reading_time)
            insert_cassandra(st, current_hour, reading_time, sensor_id, metric_value)

    print("Job Completed")

if __name__ == "__main__":
    main()
