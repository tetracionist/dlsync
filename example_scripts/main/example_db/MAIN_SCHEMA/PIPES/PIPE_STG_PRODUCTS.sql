create or replace pipe ${EXAMPLE_DB}.${MAIN_SCHEMA}.PIPE_STG_PRODUCTS
auto_ingest=true as 
copy into ${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCTS (  
    PRODUCT_NAME ,
    PRICE ,
    STOCK
)
from @${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCT_DATA_STAGE
    file_format = ${EXAMPLE_DB}.${MAIN_SCHEMA}.PRODUCT_CSV_FILE_FORMAT
    PATTERN = 'PRODUCT/\d{4}/\d{2}/\d{2}/FINANCEDATA.csv';