create or replace pipe ${EXAMPLE_DB}.${MAIN_SCHEMA}.PIPE_STG_FINANCE_DATA 
auto_ingest=true as 
copy into ${EXAMPLE_DB}.${MAIN_SCHEMA}.STG_FINANCE_DATA (  
    BRAND_CODE,
    GROSS,
    NET,
    TAX
)
from @${EXAMPLE_DB}.${MAIN_SCHEMA}.STAGE_DEV_S3 (file_format => ${EXAMPLE_DB}.${MAIN_SCHEMA}.FF_S3_CSV, PATTERN => "FINANCE/\d{4}/\d{2}/\d{2}/FINANCEDATA.csv");