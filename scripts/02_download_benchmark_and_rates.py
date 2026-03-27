"""
Скрипт загрузки бенчмарка IMOEX и ключевой ставки ЦБ РФ.

Часть 1: индекс Мосбиржи через MOEX ISS API (apimoex)
Часть 2: ключевая ставка ЦБ через SOAP-сервис DailyInfo
"""

import time
import requests
import numpy as np
import pandas as pd
import apimoex
from pathlib import Path
from xml.etree import ElementTree

# пути к файлам
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BENCHMARK_PATH = RAW_DIR / "benchmark_daily.parquet"
RATE_PATH = RAW_DIR / "risk_free_rate.parquet"


# -- Часть 1: бенчмарк IMOEX --

def download_imoex() -> pd.DataFrame:
    """
    Скачиваем индекс Мосбиржи.

    Исторически он назывался MICEX (тикер MICEXINDEXCF), а с 21.11.2017
    переименовался в IMOEX. На ISS API история MICEXINDEXCF давно слита
    в тикер IMOEX, так что качаем одним запросом за весь период.
    """
    # какие колонки нужны — явно перечисляем, чтоб не тащить мусор
    api_cols = ("TRADEDATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "VALUE")

    with requests.Session() as session:
        print("Загружаем IMOEX (2015-01-01 .. 2025-12-31)...")
        raw = apimoex.get_board_history(
            session,
            security="IMOEX",
            start="2015-01-01",
            end="2025-12-31",
            columns=api_cols,
            board="SNDX",
            market="index",
            engine="stock",
        )

    df = pd.DataFrame(raw)
    print(f"  Получено {len(df)} строк")

    if df.empty:
        raise RuntimeError("IMOEX: пустой ответ от ISS API")

    # приводим имена колонок к нижнему регистру
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"tradedate": "date"})

    df["date"] = pd.to_datetime(df["date"])
    df["index_ticker"] = "IMOEX"

    df = df.sort_values("date").reset_index(drop=True)
    df = df.drop_duplicates(subset="date", keep="last").reset_index(drop=True)

    # финальный порядок колонок
    col_order = ["date", "index_ticker", "open", "high", "low", "close", "volume", "value"]
    df = df[[c for c in col_order if c in df.columns]]

    return df


# -- Часть 2: ключевая ставка ЦБ РФ --

def download_cbr_key_rate() -> pd.DataFrame:
    """
    Скачиваем ключевую ставку через SOAP-сервис ЦБ.

    Старый XML endpoint (XML_KeyRate.asp) больше не работает,
    но SOAP-сервис DailyInfo.asmx отдает те же данные.
    Ответ содержит ежедневные записи (уже forward-filled на стороне ЦБ).
    """
    url = "https://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "http://web.cbr.ru/KeyRate",
    }
    # SOAP-запрос: fromDate и ToDate в формате ISO
    soap_body = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <KeyRate xmlns="http://web.cbr.ru/">
      <fromDate>2015-01-01</fromDate>
      <ToDate>2025-12-31</ToDate>
    </KeyRate>
  </soap:Body>
</soap:Envelope>"""

    print("Загружаем ключевую ставку ЦБ РФ (SOAP DailyInfo)...")
    resp = requests.post(url, data=soap_body, headers=headers, timeout=30)
    resp.raise_for_status()

    root = ElementTree.fromstring(resp.content)

    # ищем элементы KR в ответе (пространство имен может быть пустым)
    records = []
    for kr in root.iter():
        tag = kr.tag.split("}")[-1] if "}" in kr.tag else kr.tag
        if tag != "KR":
            continue

        dt_text, rate_text = None, None
        for child in kr:
            child_tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child_tag == "DT":
                dt_text = child.text
            elif child_tag == "Rate":
                rate_text = child.text

        if dt_text and rate_text:
            records.append({
                "date": pd.to_datetime(dt_text),
                "rate_annual": float(rate_text) / 100,  # 21% -> 0.21
            })

    if not records:
        raise RuntimeError("ЦБ: не удалось распарсить ответ SOAP")

    df = pd.DataFrame(records)
    # ЦБ отдает datetime с таймзоной UTC+03:00, а нам нужны naive-даты
    df["date"] = df["date"].dt.tz_localize(None).dt.normalize()
    df = df.sort_values("date").reset_index(drop=True)
    print(f"  Получено {len(df)} дневных записей")
    print(f"  Диапазон ставки: {df['rate_annual'].min()*100:.2f}% .. {df['rate_annual'].max()*100:.2f}%")

    return df


def make_daily_rates(df_cbr: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    ЦБ отдает ставку только по рабочим дням. Дополняем до полного
    календарного ряда (forward-fill), потом считаем дневные ставки.

    Календарные дни нужны, чтобы потом можно было мержить
    с любым набором торговых дней.
    """
    full_range = pd.date_range(start=start, end=end, freq="D")
    df_daily = pd.DataFrame({"date": full_range})

    df_daily = df_daily.merge(df_cbr, on="date", how="left")
    df_daily["rate_annual"] = df_daily["rate_annual"].ffill()

    # если первый день попал до первой записи ЦБ — заполняем назад
    df_daily["rate_annual"] = df_daily["rate_annual"].bfill()

    # пересчет в дневную ставку (252 торговых дня в году — стандарт)
    df_daily["rate_daily"] = df_daily["rate_annual"] / 252

    # логарифмическая дневная: ln(1 + r_annual) / 252
    df_daily["rate_daily_log"] = np.log(1 + df_daily["rate_annual"]) / 252

    return df_daily


def main():
    print("=" * 60)
    print("02 -- Загрузка бенчмарка IMOEX и ставки ЦБ РФ")
    print("=" * 60)
    print()

    # -- бенчмарк --
    df_bench = download_imoex()
    df_bench.to_parquet(BENCHMARK_PATH, index=False)
    print(f"\nБенчмарк сохранен: {BENCHMARK_PATH}")
    print(f"  Строк: {len(df_bench)}")
    print(f"  Период: {df_bench['date'].min().date()} .. {df_bench['date'].max().date()}")
    print(f"  Колонки: {list(df_bench.columns)}")
    print()

    time.sleep(0.5)

    # -- ключевая ставка --
    df_rate_raw = download_cbr_key_rate()
    df_rate = make_daily_rates(
        df_rate_raw,
        start="2015-01-01",
        end="2025-12-31",
    )
    df_rate.to_parquet(RATE_PATH, index=False)
    print(f"\nКлючевая ставка сохранена: {RATE_PATH}")
    print(f"  Строк: {len(df_rate)}")
    print(f"  Период: {df_rate['date'].min().date()} .. {df_rate['date'].max().date()}")
    print(f"  Колонки: {list(df_rate.columns)}")

    # итого
    print()
    print("-" * 40)
    print("Готово. Файлы:")
    print(f"  {BENCHMARK_PATH}")
    print(f"  {RATE_PATH}")


if __name__ == "__main__":
    main()
