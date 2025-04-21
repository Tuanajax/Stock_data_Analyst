from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import requests
import mysql.connector
import time
import os
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader  # Sửa import
from langchain_community.vectorstores import FAISS  # Sửa import
from langchain_community.embeddings import HuggingFaceEmbeddings  # Sửa import
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.llms import HuggingFacePipeline
from langchain.chains import RetrievalQA
from transformers import pipeline



def crawl_data():
#Cấu hình Selenium
    options = Options()
    options.headless = True  # Chạy ẩn (không mở trình duyệt)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    url ='https://cafef.vn/du-lieu/cong-bo-thong-tin.chn'
    # Thiết lập Selenium
    options = Options()
    options.headless = True  # Chạy không hiển thị trình duyệt
    driver = webdriver.Chrome(options=options)

    # URL mục tiêu
    url = "https://cafef.vn/du-lieu/cong-bo-thong-tin.chn"
    driver.get(url)

    # Chờ trang tải hoàn toàn (tùy chỉnh thời gian nếu cần)
    time.sleep(5)  # Có thể thay bằng WebDriverWait nếu biết element cụ thể

    # Lấy HTML của trang
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")

    # Tìm <div> chứa bảng
    div = soup.find("div", class_="wrapper-table-information-disclosure")
    if not div:
        print("Không tìm thấy <div> với class 'wrapper-table-information-disclosure'!")
        driver.quit()
        exit()

    # Tìm bảng trong <div>
    table = div.find("table")
    if not table:
        print("Không tìm thấy bảng trong <div>!")
        driver.quit()
        exit()

    # Trích xuất tiêu đề từ <thead>
    thead = table.find("thead")
    headers = [th.get_text(strip=True) for th in thead.find("tr").find_all("th")] if thead else []
    headers = [" ".join(h.split()) for h in headers]  # Làm sạch tiêu đề

    # Trích xuất dữ liệu từ <tbody>
    tbody = table.find("tbody", id="render-table-information-disclosure")
    if not tbody:
        print("Không tìm thấy <tbody>! Dùng toàn bộ bảng.")
        tbody = table

    rows = []
    pdf_urls = []
    pdf_paths = []
    download_dir = "/home/tuanvu/Stock_VNI/financial_reports"
    os.makedirs(download_dir, exist_ok=True)

    for  index, tr  in enumerate(tbody.find_all("tr")):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        pdf_url = tr.find("a")["href"] if tr.find("a") else ""
        pdf_path = ""
        if pdf_url and pdf_url.startswith("http"):
            try:
                response = requests.get(pdf_url, stream=True)
                response.raise_for_status()
                pdf_path = os.path.join(download_dir, f"{cells[0]}_{index}.pdf")  # VD: BCR_0.pdf
                with open(pdf_path, "wb") as f:
                    f.write(response.content)
                print(f"Đã tải PDF vào: {pdf_path}")
                cells[-1] = pdf_url  # Giữ URL gốc trong cột "Tải về"
            except requests.RequestException as e:
                print(f"Lỗi khi tải PDF từ {pdf_url}: {e}")
                cells[-1] = pdf_url
        rows.append(cells)
        pdf_urls.append(pdf_url)
        pdf_paths.append(pdf_path if pdf_path else None)

    # Tạo DataFrame
    df = pd.DataFrame(rows, columns=headers)
    print("Dữ liệu bảng từ URL:")
    print(df)

    # Kết nối MySQL
    try:
        conn = mysql.connector.connect(
            host="localhost",         # Thay bằng host của bạn
            user="root",     # Thay bằng username MySQL
            password="Eninoskybaby94$", # Thay bằng password MySQL
            database="STOCK"  # Thay bằng tên database
        )
        cursor = conn.cursor()
        # Chèn dữ liệu
        insert_query = """
        INSERT INTO financial_reports (ma, ten_cong_ty, loai_bao_cao, quy_4_2024, quy_4_2023, thay_doi, thoi_gian_gui, tai_ve, pdf_path)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for i, row in enumerate(rows):
            data = tuple(row) + (pdf_paths[i],)
            cursor.execute(insert_query, data)

        conn.commit()
        print("Dữ liệu và đường dẫn PDF đã được lưu vào MySQL!")

    except mysql.connector.Error as err:
        print(f"Lỗi MySQL: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
    # Đóng trình duyệt
    driver.quit()
conn = mysql.connector.connect(
        host="localhost",         # Thay bằng host của bạn
        user="root",     # Thay bằng username MySQL
        password="Eninoskybaby94$", # Thay bằng password MySQL
        database="STOCK"  # Thay bằng tên database
    )
cursor = conn.cursor()


# Load all PDFs from MySQL and create vector store
cursor.execute("SELECT ma, pdf_path FROM financial_reports WHERE ma='TIX'")
pdf_records = cursor.fetchall()
if not pdf_records:
    print("No records found for ma='BCR'")
else:
    for ma, pdf_path in pdf_records:
        temp_pdf_path = pdf_path
        loader = PyPDFLoader(temp_pdf_path)
        # Split the PDF into Pages
        pages = loader.load_and_split()
# Define chunk size, overlap and separators
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=1024,
    chunk_overlap=64,
    separators=['\n\n', '\n', '(?=>\. )', ' ', '']
)
docs  = text_splitter.split_documents(pages)
print(f"Đã chia thành {len(docs)} đoạn văn bản.")
#EMEBEDDING
print(temp_pdf_path)
print(loader)


# %%
