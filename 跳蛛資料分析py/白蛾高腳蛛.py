import pandas as pd
import re

# === 參數 ===
file_path = "Heteropoda venatoria.csv"   # 原始 CSV
output_file = "白蛾高腳蛛_tableau_年統計.csv"  # 輸出整合檔
keep_missing_dates = True  # True=保留沒有年份的列；False=刪掉沒有年份的列

# === 讀檔（中文 Windows 常見編碼）+ 欄名清理 ===
df = pd.read_csv(file_path, encoding="cp950")
df.columns = [re.sub(r"\s+", "", col) for col in df.columns]

# === 篩選需要欄位 ===
cols_needed = ["scientificName", "taibif_county_zh", "decimalLongitude", "decimalLatitude", "eventDate"]
df_clean = df[cols_needed].copy()

# === 時間 -> 只取年份 ===
dt = pd.to_datetime(df_clean["eventDate"], errors="coerce", utc=False)
df_clean["year"] = dt.dt.year  # 只保留年份（整數）；解析失敗為 NaN

# 是否刪掉沒有年份的列
if not keep_missing_dates:
    df_clean = df_clean.dropna(subset=["year"])

# === 數量欄位（每筆=1）===
df_clean["count"] = 1

# === 最終欄位（Tableau 友善：直接用 year 當時間）===
df_clean = df_clean[[
    "scientificName",      # 白蛾高腳蛛學名
    "taibif_county_zh",    # 分布縣市
    "decimalLongitude",    # 經度
    "decimalLatitude",     # 緯度
    "year",                # 年份（僅年份）
    "count"                # 數量
]]

# === 年度統計（因只看年份，不做月份統計）===
year_stats = (
    df_clean.groupby("year", dropna=False)["count"]
    .sum()
    .reset_index(name="total_count")
    .sort_values("year")
)

county_stats = (
    df_clean.groupby("taibif_county_zh", dropna=False)["count"]
    .sum()
    .reset_index(name="total_count")
    .sort_values("total_count", ascending=False)
)

# === 輸出整合 CSV（原始清理表 + 年度統計 + 縣市統計）===
with open(output_file, "w", encoding="utf-8-sig") as f:
    f.write("=== 原始觀測資料（只含年份） ===\n")
df_clean.to_csv(output_file, mode="a", index=False, encoding="utf-8-sig")

with open(output_file, "a", encoding="utf-8-sig") as f:
    f.write("\n\n=== 年度統計 ===\n")
    year_stats.to_csv(f, index=False)
    f.write("\n\n=== 縣市分布統計 ===\n")
    county_stats.to_csv(f, index=False)

print(f"整合檔案已輸出：{output_file}")


