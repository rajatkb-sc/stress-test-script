echo "CREATING_DATASET ..."
bq query --use_legacy_sql=false < create_data.sql

echo "EXTRACTING_DATASET ..."
bq extract --destination_format=NEWLINE_DELIMITED_JSON maximal-furnace-783:rajat.stress_test_dataset "gs://rajat-temp/stress_test/output.json"

echo "DOWNLOADING DATASET ..."
gsutil cp gs://rajat-temp/stress_test/output.json .

echo "REMOVING DATASET FROM STORAGE ..."
gsutil rm gs://rajat-temp/stress_test/output.json

echo "CLEANING_UP_DATASET ..."
bq rm -f "maximal-furnace-783.rajat.stress_test_dataset"