import akshare as ak
import pandas as pd

# 获取A股上市公司基本信息
stock_info = ak.stock_info_a_code_name()

# 获取最新的财务指标数据（例如沪深300成分股作为演示）
hs300 = ak.index_stock_cons(symbol="000300")  # 沪深300成分股
codes = hs300["品种代码"].tolist()

result = []

for code in codes:
    try:
        # 获取财务指标
        fin = ak.stock_a_lg_indicator(symbol=code)
        latest = fin.iloc[-1]

        roe = latest["ROE摊薄(%)"]
        pe = latest["市盈率(PE，TTM)"]
        pb = latest["市净率(PB)"]

        # 筛选条件：ROE>10%，PE<30，PB<3
        if roe > 10 and pe > 0 and pe < 30 and pb < 3:
            stock_name = hs300.loc[hs300["品种代码"]==code, "品种名称"].values[0] if len(hs300.loc[hs300["品种代码"]==code, "品种名称"].values) > 0 else "未知"
            result.append({
                "代码": code,
                "名称": stock_name,
                "ROE": roe,
                "PE": pe,
                "PB": pb
            })
    except:
        continue

df = pd.DataFrame(result).sort_values(by="ROE", ascending=False)
df.to_excel("价值投资候选股.xlsx", index=False)

print("筛选完成，结果已保存到 价值投资候选股.xlsx")
