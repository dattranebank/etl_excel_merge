import azure.functions as func
import logging
import os
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient
from handle import clean_columns
from datetime import datetime
from zoneinfo import ZoneInfo
import gc

app = func.FunctionApp()

def check_blob_schema(raw_container, subfolder):
    """Hợp nhất schema có GIỮ THỨ TỰ:
       - lấy thứ tự cột file đầu tiên làm chuẩn
       - cột mới ở các file sau sẽ append vào cuối theo thứ tự gặp
       - 'Nguồn file' luôn ở cuối
    """
    logging.info(f"🔍 Đang quét schema tất cả file Excel trong 'raw/{subfolder}'...")
    ordered_cols = []
    seen = set()

    # Duyệt file theo thứ tự tên để kết quả ổn định
    blob_list = sorted(raw_container.list_blobs(name_starts_with=subfolder), key=lambda x: x.name)
    excel_files = [b for b in blob_list if b.name.endswith((".xlsx", ".xls"))]

    if not excel_files:
        logging.warning(f"⚠️ Không tìm thấy file Excel nào trong 'raw/{subfolder}'")
        return []

    for blob in excel_files:
        try:
            blob_client = raw_container.get_blob_client(blob.name)
            stream = io.BytesIO(blob_client.download_blob().readall())
            df = pd.read_excel(
                stream,
                sheet_name="DSDH",
                skiprows=2,
                header=0,
                nrows=0,  # đọc header thôi
                engine="openpyxl"
            )
            for c in df.columns.tolist():  # giữ đúng thứ tự trong Excel
                if c not in seen:
                    seen.add(c)
                    ordered_cols.append(c)
        except Exception as e:
            logging.error(f"⚠️ Không đọc được cột của {blob.name}: {e}")

    # Đảm bảo 'Nguồn file' nằm cuối cùng
    if "Nguồn file" not in ordered_cols:
        ordered_cols.append("Nguồn file")

    logging.info(f"📊 Tổng số cột sau khi union (giữ thứ tự): {len(ordered_cols)}")
    return ordered_cols

def process_to_parquet(raw_container, staging_container, subfolder, parquet_name, all_columns):
    """Đọc toàn bộ file Excel trong raw/<subfolder>, chuẩn hóa schema, ghi thành 1 file Parquet."""
    logging.info(f"📘 Đang xử lý file trong 'raw/{subfolder}'...")
    
    writer = None
    file_count = 0
    blob_list = sorted(raw_container.list_blobs(name_starts_with=subfolder), key=lambda x: x.name)
    excel_files = [b for b in blob_list if b.name.endswith((".xlsx", ".xls"))]

    if not excel_files:
        logging.warning(f"⚠️ Không có file Excel nào để xử lý trong 'raw/{subfolder}'")
        return 0

    for blob in excel_files:
        logging.info(f"📥 Reading file: {blob.name}")
        try:
            blob_client = raw_container.get_blob_client(blob.name)
            stream = io.BytesIO(blob_client.download_blob().readall())
            df = pd.read_excel(
                stream,
                sheet_name="DSDH",
                skiprows=2,
                header=0,
                dtype_backend="pyarrow",
                engine="openpyxl"
            )

            df["Nguồn file"] = blob.name  # Thêm cột Nguồn file

            # Bổ sung cột còn thiếu
            for col in all_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            df = df[all_columns]

            df = clean_columns(df)  # Làm sạch tên cột

            # Ép toàn bộ cột sang string để tránh lỗi schema mismatch
            for col in df.columns:
                df[col] = df[col].astype("string")

            # Chuyển sang Arrow Table
            table = pa.Table.from_pandas(df, preserve_index=False)

            # Ghi file Parquet (chỉ khởi tạo writer 1 lần)
            if writer is None:
                output_stream = io.BytesIO()
                writer = pq.ParquetWriter(output_stream, table.schema, compression="snappy")

            writer.write_table(table)
            file_count += 1

            del df, table
            gc.collect()

        except Exception as e:
            logging.error(f"❌ Lỗi khi xử lý {blob.name}: {e}")

    # Đóng writer và upload file Parquet
    if writer:
        writer.close()
        output_stream.seek(0)
        staging_container.upload_blob(
            name=parquet_name,
            data=output_stream.getvalue(),
            overwrite=True
        )
        logging.info(f"✅ Merged Parquet uploaded to 'staging/{parquet_name}'")
        output_stream.close()
    else:
        logging.info(f"❌ Không có dữ liệu nào được ghi cho {subfolder}")

    return file_count

@app.function_name(name="merge_excel_files")
@app.route(
    route="merge_excel_files",
    methods=["POST"],
    auth_level=func.AuthLevel.FUNCTION
)
def merge_excel_to_parquet(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("=== Start merging Excel files from 'raw' container to Parquet ===")

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
            "dms": ["dms/dms17"]
        }

        # 4. Loop qua từng hệ thống và báo cáo
        for system, reports in systems.items():
            for report in reports:
                subfolder = f"{report}/year={year}"
                logging.info(f"🔎 Processing {system} → {subfolder}")

                # 4a. Quét schema
                all_columns = check_blob_schema(raw_container, subfolder)
                if not all_columns:
                    continue

                # 4b. Xử lý và ghi Parquet
                report_name = report.split("/")[-1]  # "dms17"
                parquet_name = f"{subfolder}/{report_name}_latest.parquet"
                file_count = process_to_parquet(raw_container, staging_container, subfolder, parquet_name, all_columns)

                if file_count > 0:
                    logging.info(f"✅ Hoàn tất ghi {file_count} file vào 'staging/{parquet_name}'")
                else:
                    logging.info(f"⚠️ Không có dữ liệu hợp lệ để merge cho {subfolder}")

        return func.HttpResponse("✅ Merge completed successfully", status_code=200)

    except Exception as e:
        logging.error(f"❌ Error during merge: {str(e)}")
        return func.HttpResponse(f"❌ Error: {str(e)}", status_code=500)