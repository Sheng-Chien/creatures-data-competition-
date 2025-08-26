import pandas as pd
import re

# === 1. 讀取原始資料 ===
file_path = "Hasarius adansoni_raw_data.csv"  # 你可以修改為自己的本地路徑
df = pd.read_csv(file_path, encoding='iso-8859-1')

# === 2. 初步資料清理 ===
# 移除重要欄位缺值、重複值
df = df.dropna(subset=['scientificName', 'eventDate', 'decimalLatitude', 'decimalLongitude'])
df['organismQuantity'] = 1  # 每筆資料視為 1 次觀測
df = df.drop_duplicates(subset=['scientificName', 'eventDate', 'decimalLatitude', 'decimalLongitude'])

# 清除文字亂碼與特殊符號
def clean_text(val):
    if isinstance(val, str):
        val = re.sub(r'[^\w\s\u4e00-\u9fff\.-]', '', val)
        return val.strip()
    return val

df = df.applymap(clean_text)

# 經緯度轉數值 + 時間轉換
df = df[(df['decimalLatitude'].astype(float).between(-90, 90)) & 
        (df['decimalLongitude'].astype(float).between(-180, 180))]
df['eventDate'] = pd.to_datetime(df['eventDate'], errors='coerce', utc=True)
df['year'] = df['eventDate'].dt.year

# === 3. 中文俗名欄位補充 ===
df['taibif_vernacularName'] = '阿當氏哈蘇跳蛛'

# === 4. 將英文縣市轉為中文 ===
county_en_to_zh = {
    'Taipei': '台北市', 'New Taipei': '新北市', 'Taoyuan': '桃園市',
    'Taichung': '台中市', 'Tainan': '台南市', 'Kaohsiung': '高雄市',
    'Keelung': '基隆市', 'Hsinchu': '新竹市', 'Chiayi': '嘉義市',
    'Hsinchu County': '新竹縣', 'Miaoli County': '苗栗縣',
    'Changhua County': '彰化縣', 'Nantou County': '南投縣',
    'Yunlin County': '雲林縣', 'Chiayi County': '嘉義縣',
    'Pingtung County': '屏東縣', 'Yilan County': '宜蘭縣',
    'Hualien County': '花蓮縣', 'Taitung County': '台東縣',
    'Penghu County': '澎湖縣', 'Kinmen County': '金門縣',
    'Lienchiang County': '連江縣'
}
df['縣市'] = df['stateProvince'].map(county_en_to_zh)

# === 5. 建立清理後資料集，排除 NULL 縣市 ===
df_cleaned = df[['eventDate', 'year', '縣市', 'decimalLatitude', 'decimalLongitude',
                 'scientificName', 'taibif_vernacularName', 'organismQuantity']].copy()
df_cleaned = df_cleaned.dropna(subset=['縣市'])

# === 6. 計算年度 + 各縣市觀測數量 ===
species_by_year = (
    df_cleaned.dropna(subset=['year'])
    .groupby(['year', '縣市'])['organismQuantity']
    .sum()
    .reset_index()
    .sort_values(['縣市', 'year'])
)

# === 7. 匯出為乾淨的 CSV ===
output_path = "final_Hasarius_adansoni_by_year_cleaned.csv"
species_by_year.to_csv(output_path, index=False, encoding='utf-8-sig')
print("✅ 年度 × 縣市 統計已匯出為：", output_path)
