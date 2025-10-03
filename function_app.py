import azure.functions as func
import logging
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# Timer Trigger: chạy 3 lần/ngày (07h, 13h, 19h)
@app.schedule(schedule="0 */10 * * * *", arg_name="myTimer", run_on_startup=True)
def merge_excel_to_csv(myTimer: func.TimerRequest) -> None:
    logging.info("=== Start merging Excel files from 'raw' container ===")

    try:
        # 1. Kết nối Blob
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Source: raw
        raw_container = blob_service_client.get_container_client("raw")
        # Destination: staging
        staging_container = blob_service_client.get_container_client("staging")

        # 2. Lấy danh sách file Excel trong container raw
        blob_list = raw_container.list_blobs()
        excel_files = [b for b in blob_list if b.name.endswith((".xlsx", ".xls"))]

        if not excel_files:
            logging.info("⚠️ Không tìm thấy file Excel nào trong 'raw'")
            return

        merged_df = None

        # 3. Đọc từng file Excel
        for blob in excel_files:
            logging.info(f"Reading file: {blob.name}")
            blob_client = raw_container.get_blob_client(blob.name)
            stream = io.BytesIO(blob_client.download_blob().readall())

            # Bỏ 2 dòng đầu, dùng dòng thứ 3 làm header
            df = pd.read_excel(stream, skiprows=2, header=0, engine="openpyxl")
            print(df)

            if merged_df is None:
                merged_df = df
            else:
                merged_df = pd.concat([merged_df, df], ignore_index=True)

        # 4. Xuất CSV
        output_stream = io.StringIO()
        merged_df.to_csv(output_stream, index=False, encoding="utf-8-sig")

        # 5. Upload CSV sang container staging (folder dms)
        output_blob_name = "dms/merged_file.csv"
        staging_container.upload_blob(
            name=output_blob_name,
            data=output_stream.getvalue(),
            overwrite=True
        )

        logging.info(f"✅ Merged CSV uploaded to 'staging/{output_blob_name}'")

    except Exception as e:
        logging.error(f"❌ Error during merge: {str(e)}")
