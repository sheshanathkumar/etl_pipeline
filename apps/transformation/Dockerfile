FROM apache/spark:3.5.3
USER root
RUN pip install psycopg2-binary pandas openpyxl xlsxwriter
WORKDIR /apps/transformation
COPY db_utils.py transformation.py wait_for_extraction.sh ./
RUN chmod +x /apps/transformation/wait_for_extraction.sh
