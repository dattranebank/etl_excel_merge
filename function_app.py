import azure.functions as func
import logging
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from handle import *
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

app = func.FunctionApp()


# Timer Trigger: chạy 07h, 13h, 19h giờ VN
@app.schedule(schedule="0 0 7,13,19 * * *", arg_name="myTimer", run_on_startup=True)
def merge_excel_to_csv(myTimer: func.TimerRequest) -> None:
    logging.info("=== Start merging Excel files from 'raw' container ===")

    try:
        # 1. Kết nối Blob
        connection_string = os.environ["AzureWebJobsStorage"]
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        raw_container = blob_service_client.get_container_client("raw")
        staging_container = blob_service_client.get_container_client("staging")

        # 2. Lấy năm hiện tại theo giờ Việt Nam
        vn_now = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
        year = vn_now.year

        # 3. Định nghĩa metadata: hệ thống & báo cáo
        systems = {
            "dms": ["dms/dms17"]  # sau này bổ sung thêm misa, interncare ở đây
        }

        # 4. Loop qua từng hệ thống và báo cáo
        for system, reports in systems.items():
            for report in reports:
                subfolder = f"{report}/year={year}"
                logging.info(f"🔎 Processing {system} → {subfolder}")

                # 4a. Lấy danh sách file Excel trong raw/<report>/year=<year>
                blob_list = raw_container.list_blobs(name_starts_with=subfolder)
                excel_files = [b for b in blob_list if b.name.endswith((".xlsx", ".xls"))]

                if not excel_files:
                    logging.warning(f"⚠️ Không tìm thấy file Excel nào trong 'raw/{subfolder}'")
                    continue

                all_data = []

                # 4b. Đọc từng file từ Blob
                for blob in excel_files:
                    logging.info(f"📥 Reading file: {blob.name}")
                    blob_client = raw_container.get_blob_client(blob.name)
                    stream = io.BytesIO(blob_client.download_blob().readall())

                    try:
                        df = pd.read_excel(
                            stream, skiprows=2, header=0, sheet_name="DSDH", engine="openpyxl"
                        )
                        df["Nguồn file"] = blob.name
                        df = clean_columns(df)  # hàm trong handle.py
                        all_data.append(df)
                    except Exception as e:
                        logging.error(f"❌ Lỗi khi xử lý {blob.name}: {e}")

                if not all_data:
                    logging.info(f"⚠️ Không có dữ liệu hợp lệ để merge cho {subfolder}")
                    continue

                # 4c. Merge tất cả DataFrame
                df_all = pd.concat(all_data, ignore_index=True)
                cols = [c for c in df_all.columns if c != "Nguồn file"] + ["Nguồn file"]
                df_all = df_all[cols]

                # 5a. Xuất CSV ra memory
                output_csv_stream = io.StringIO()
                df_all.to_csv(output_csv_stream, index=False, encoding="utf-8-sig")

                # 5b. Xuất Excel ra memory
                output_excel_stream = io.BytesIO()
                with pd.ExcelWriter(output_excel_stream, engine="openpyxl") as writer:
                    df_all.to_excel(writer, index=False, sheet_name="Sheet1")
                output_excel_stream.seek(0)

                # 6. Upload CSV và Excel sang Blob staging/<report>/year=<year>
                report_name = report.split("/")[-1]  # "dms17"
                output_csv_name = f"{subfolder}/{report_name}_latest.csv"
                output_excel_name = f"{subfolder}/{report_name}_latest.xlsx"

                staging_container.upload_blob(
                    name=output_csv_name,
                    data=output_csv_stream.getvalue(),
                    overwrite=True
                )
                staging_container.upload_blob(
                    name=output_excel_name,
                    data=output_excel_stream.getvalue(),
                    overwrite=True
                )

                logging.info(f"✅ Merged CSV uploaded to 'staging/{output_csv_name}'")
                logging.info(f"✅ Merged Excel uploaded to 'staging/{output_excel_name}'")

    except Exception as e:
        logging.error(f"❌ Error during merge: {str(e)}")
