from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

LOCALHOST ='localhost:9092'
TOPIC ='user_events'

#I don't remember if i need a master
spark = {
        SparkSession.builder
        .appName('UserEventConsumer')
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0'
        .getOrCreate()
        }


spark.sparkContext.setLogLevel('WARN')


#EVENT_TYPES = ["login", "logout", "page_view", "click", "search", "add_to_cart", "remove_from_cart"]
#PAGES = ["home", "products", "product_detail", "cart", "checkout", "profile", "settings", "help"]
#DEVICES = ["desktop", "mobile", "tablet"]
#BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]

## This is the event record
#    event = {
#        "event_id": fake.uuid4(),
#        "user_id": user_id,
#        "session_id": session_id,
#        "event_type": event_type,
#        "timestamp": datetime.utcnow().isoformat() + "Z",
#        "page": random.choice(PAGES),
#        "device": random.choice(DEVICES),
#        "browser": random.choice(BROWSERS),
#        "ip_address": fake.ipv4(),
#        "country": fake.country_code(),
#        "city": fake.city(),
#    }

#These should not be all true 
event_schema = StructType([
    StructField('event_id', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('session_id', StringType(), True),
    StructField('event_type', StringType(), True),
    StructField('timestamp', TimeStampType(), True),
    StructField('page', StringType(), True),
    StructField('device', StringType(), True),
    StructField('browser', StringType(), True),
    StructField('ip_address', StringType(), True),
    StructField('country', StringType(), True),
    StructField('city', StringType(), True)
    )]

event_producer_stream = {
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', LOCALHOST)
    .option('subscribe', TOPIC)
    .option('startingOffsets', 'latest')
    .load()
    }


