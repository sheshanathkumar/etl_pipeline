import uuid
import logging
from pyspark.sql import SparkSession
from db_utils import insert_job, update_job_status

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
import json, glob, time, os, logging, string, random
from faker import Faker
from decimal import Decimal, getcontext
from datetime import datetime


fake = Faker()

#logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

logger.info("Starting spark session for credit card extraction")

spark = (
    SparkSession.builder
    .appName("credit-card-extraction")
    .config("spark.rpc.message.maxSize", "256")
    .config("spark.driver.maxResultSize", "1g")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)

def main():
    logger.info("Starting data extraction process...")
    start_time = time.perf_counter()
    extraction_process()
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    logger.info(f"Data extraction process completed in {elapsed_time:.2f} seconds.")


def random_string(max_len):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, max_len)))


def random_decimal(precision, scale):
    getcontext().prec = precision
    max_int_part_digits = precision - scale
    max_int = 10**max_int_part_digits - 1 
    int_part = random.randint(0, max_int)
    frac_part = random.randint(0, 10**scale - 1)
    fmt = f"{{int_part}}.{{frac_part:0{scale}d}}" if scale > 0 else "{int_part}"
    return Decimal(fmt.format(int_part=int_part, frac_part=frac_part))

def extraction_process():

    job_uuid = uuid.uuid4().hex
    job_id = job_uuid[:5]
    logger.info(f"Generated job ID: {job_id}")

    raw_path = f"/data/raw/credit_card/job_id={job_id}"

    insert_job(job_id, "credit_card", raw_path)
    logger.info(f"Inserted job {job_id} into job tracking database")

    with open('user.txt', 'r') as data_file:
        logger.info(f"Loading users from source file...")
        users = data_file.readlines()

    
    with open('account.txt', 'r') as account_file:
        logger.info(f"Loading accounts file from source file...")
        accounts = account_file.readlines()

    with open('card.txt', 'r') as card_file:
        logger.info(f"Loading accounts file from source file...")
        cards = card_file.readlines()

    with open('merch.txt', 'r') as merch_file:
        logger.info(f"Loading accounts file from source file...")
        merchs = merch_file.readlines()
    
    with open('credit_card_spent.json', 'r') as f:
        logger.info(f"Loading schema from schema...")
        schema_json = json.load(f)



    fields = []
    for field in schema_json:
        field_name = field['field_name']
        datatype = field['datatype']
        precision = field.get('precision')
        scale = field.get('scale')
        if datatype == 'STRING':
            fields.append(StructField(field_name, StringType(), True))
        elif datatype == 'INTEGER':
            fields.append(StructField(field_name, IntegerType(), True))
        elif datatype == 'DECIMAL':
            fields.append(StructField(field_name, DecimalType(precision, scale), True))
        elif datatype == 'TIMESTAMP':
            fields.append(StructField(field_name, TimestampType(), True))
        else:
            fields.append(StructField(field['field_name'], StringType(), True))

    schema= StructType(fields)
    data = []
    for _ in range(1000000):
        row = []
        for field in schema_json:
            field_name = field['field_name']
            datatype = field['datatype'].upper()
            maxLength = field.get('maxLength')
            precision = field.get('precision')
            scale = field.get('scale')
            
            if field_name == 'person_name':
                val = random.choice(users).strip()
            elif field_name == "acct_num":
                val = fake.iban().strip()
            elif field_name == "card_company":
                val = random.choice(cards).strip()
            elif field_name == 'card_number':
                val = random.choice(accounts).strip()
            elif field_name == 'txn_datetime':
                dt = fake.date_time_this_year()
                val = dt.strftime("%Y-%m-%d %H:%M:%S")
            elif field_name == 'merch_name':
                val = random.choice(merchs).strip()
            elif field_name == 'merch_cat':
                val = random.choice(['Grocery', 'Electronics', 'Clothing', 'Dining', 'Travel', 'Health'])
            elif field_name == 'txn_amount':
                val = random_decimal(7, 2)
            elif field_name == 'currency':
                val = 'INR'
            elif field_name == 'pay_meth':
                val = random.choice(['UPI','Credit Card', 'Debit Card', 'Mobile Payment', 'Bank Transfer','NEFT','RTGS'])
            elif field_name == 'city':
                val = fake.city()
            elif field_name == 'txn_type':
                val = random.choice(['Purchase', 'Refund', 'Withdrawal', 'Deposit'])
            elif field_name == 'bill_cyc':
                val = fake.month_name()
            elif field_name == 'txn_status':
                val = random.choice(['Approved', 'Declined', 'Pending', 'Reversed'])

            else:
                val = None
            row.append(val)
        data.append(tuple(row))

    num_partitions = 64
    rdd = spark.sparkContext.parallelize(data, num_partitions)
    df = spark.createDataFrame(rdd, schema)

    try :
        logger.info("data extraction completed.. ")
        logger.info("preparing csv file")

        curr_time = datetime.now().strftime("%Y%m%d_%H%M%S")

        df.write.mode("overwrite").option("header", "true").option("maxRecordsPerFile", 200000).csv(raw_path)

        logger.info("csv file prepared and saved to raw_files directory")
        logger.info("file saved in directory: "+ raw_path)

        update_job_status(job_id, "EXTRACTED")
        logger.info(f"Job {job_id} extraction completed successfully")

        cleanup_temp_files(raw_path)
    except Exception as e:
        logger.error(f"Error during data extraction: {e}")
        update_job_status(job_id, "FAILED")

def cleanup_temp_files(output_path):
    try :
        folder_path = output_path
        logger.info(f"Cleaning up temporary files in {folder_path}...")

        # removing all non csv files
        for fname in os.listdir(folder_path):
            nonCSV_file = os.path.join(folder_path, fname)
            if os.path.isfile (nonCSV_file) and not fname.endswith('.csv'):
                os.remove(nonCSV_file)
                logger.info(f"Removed temporary file: {nonCSV_file}") 
        
        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not csv_files:
            logger.warning("No CSV files found to rename.")
            return
        if len(csv_files) > 1:
            logger.warning("Multiple CSV files found. Renaming the first one found.")
        
        old_csv = csv_files[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"credit_card_spent_{timestamp}.csv"
        new_path = os.path.join(folder_path, new_name)
        os.rename(old_csv, new_path)
    except Exception as e:
        logger.error(f"Error during cleanup of temporary files: {e}")
 

if __name__ == "__main__":
    main()