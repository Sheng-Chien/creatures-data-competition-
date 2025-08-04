import pandas as pd
import re

# 1. 讀取 CSV，使用 Big5 解碼處理中文亂碼
file_path = 'spider_Salticidae_analsis.csv'  # 替換成你的路徑
df = pd.read_csv(file_path, encoding='big5')

# 2. 資料清理
df = df.dropna(subset=['scientificName', 'eventDate', 'decimalLatitude', 'decimalLongitude'])
df['organismQuantity'] = 1
df = df.drop_duplicates(subset=['scientificName', 'eventDate', 'decimalLatitude', 'decimalLongitude'])

# 清理文字欄位
def clean_text(val):
    if isinstance(val, str):
        val = re.sub(r'[^\w\s\u4e00-\u9fff\.-]', '', val)
        return val.strip()
    return val

df = df.applymap(clean_text)
df = df[(df['decimalLatitude'].between(-90, 90)) & (df['decimalLongitude'].between(-180, 180))]
df['eventDate'] = pd.to_datetime(df['eventDate'], errors='coerce')
df['year'] = df['eventDate'].dt.year
df['taibif_vernacularName'] = df['taibif_vernacularName'].fillna('未知')

# 建立清理後的資料集
df_cleaned = df[['eventDate', 'year', 'taibif_county_zh', 'decimalLatitude', 'decimalLongitude',
                 'scientificName', 'taibif_vernacularName', 'organismQuantity']].copy()

# 儲存清理後資料
df_cleaned.to_csv("cleaned_spider_data.csv", index=False, encoding='utf-8-sig')

# 3. 跳蛛俗名每年數量趨勢
species_by_year = (
    df_cleaned.dropna(subset=['year'])
    .groupby(['year', 'taibif_vernacularName'])['organismQuantity']
    .sum()
    .reset_index()
    .sort_values(['taibif_vernacularName', 'year'])
)
species_by_year.to_csv("species_by_year.csv", index=False, encoding='utf-8-sig')

# 4. 各縣市跳蛛觀測數量
species_by_county = (
    df_cleaned.groupby('taibif_county_zh')['organismQuantity']
    .sum()
    .reset_index()
    .rename(columns={'taibif_county_zh': '縣市', 'organismQuantity': '跳蛛觀測總數'})
)
species_by_county.to_csv("species_by_county.csv", index=False, encoding='utf-8-sig')

# 5. 加入經緯度中心點（供 Tableau 地圖使用）
county_geo_mapping = {
    "台北市": (25.0330, 121.5654), "新北市": (25.0169, 121.4628), "桃園市": (24.9937, 121.3000),
    "台中市": (24.1477, 120.6736), "台南市": (23.0000, 120.2269), "高雄市": (22.6273, 120.3014),
    "基隆市": (25.1292, 121.7419), "新竹市": (24.8039, 120.9647), "嘉義市": (23.4801, 120.4491),
    "新竹縣": (24.8385, 121.0022), "苗栗縣": (24.5602, 120.8210), "彰化縣": (24.0685, 120.5575),
    "南投縣": (23.9609, 120.9719), "雲林縣": (23.7092, 120.4313), "嘉義縣": (23.4589, 120.5740),
    "屏東縣": (22.5514, 120.5487), "宜蘭縣": (24.7021, 121.7378), "花蓮縣": (23.9872, 121.6015),
    "台東縣": (22.7583, 121.1440), "澎湖縣": (23.5714, 119.5790), "金門縣": (24.4321, 118.3171),
    "連江縣": (26.1608, 119.9489)
}

# 加入經緯度欄位
species_by_county['Latitude'] = species_by_county['縣市'].map(lambda x: county_geo_mapping.get(x, (None, None))[0])
species_by_county['Longitude'] = species_by_county['縣市'].map(lambda x: county_geo_mapping.get(x, (None, None))[1])

# 輸出 Tableau 用地圖資料
species_by_county.to_csv("tableau_縣市跳蛛分布_含經緯度.csv", index=False, encoding='utf-8-sig')

# 預覽結果
print("✅ 清理後資料共筆數：", len(df_cleaned))
print("🔄 年度趨勢檔案輸出：species_by_year.csv")
print("🗺️ 縣市統計檔案輸出：species_by_county.csv")
print("🌍 地圖定位檔案輸出：tableau_縣市跳蛛分布_含經緯度.csv")
