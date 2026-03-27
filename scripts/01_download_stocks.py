"""
Скрипт для загрузки дневных OHLCV-данных по 30 российским акциям с MOEX ISS API.
Сохраняет: ohlcv_daily.parquet, instruments.csv, trading_calendar.parquet.
"""

import time
from pathlib import Path

import pandas as pd
import requests
import apimoex

# пути к папкам проекта
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
META_DIR = PROJECT_ROOT / "data" / "meta"

RAW_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

# период выгрузки
START_DATE = "2015-01-01"
END_DATE = "2025-12-31"

# 30 тикеров — голубые фишки и крупные публичные компании на MOEX
TICKERS_30 = [
    "SBER", "GAZP", "LKOH", "GMKN", "NVTK",
    "ROSN", "PLZL", "YNDX", "TCSG", "MGNT",
    "SNGS", "CHMF", "ALRS", "MOEX", "MTSS",
    "VTBR", "NLMK", "PHOR", "TATN", "PIKK",
    "POLY", "IRAO", "RUAL", "MAGN", "AFKS",
    "FIVE", "RTKM", "FEES", "FLOT", "OZON",
]

# какие колонки забираем с MOEX ISS (history endpoint)
MOEX_COLUMNS = (
    "TRADEDATE", "SECID",
    "OPEN", "HIGH", "LOW", "CLOSE",
    "LEGALCLOSEPRICE", "WAPRICE",
    "VOLUME", "VALUE", "NUMTRADES",
)

# маппинг колонок MOEX -> наши названия (lowercase snake_case)
RENAME_MAP = {
    "TRADEDATE": "date",
    "SECID": "ticker",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
    "LEGALCLOSEPRICE": "close_official",
    "WAPRICE": "waprice",
    "VOLUME": "volume",
    "VALUE": "value",
    "NUMTRADES": "num_trades",
}

# метаданные по каждому тикеру — заполняем вручную
INSTRUMENTS = [
    ("SBER", "Сбербанк", "Финансы", "1997-01-01", ""),
    ("GAZP", "Газпром", "Нефть и газ", "2006-02-08", ""),
    ("LKOH", "ЛУКОЙЛ", "Нефть и газ", "1997-01-01", ""),
    ("GMKN", "Норильский никель", "Горная добыча", "2001-01-01", ""),
    ("NVTK", "НОВАТЭК", "Нефть и газ", "2005-12-28", ""),
    ("ROSN", "Роснефть", "Нефть и газ", "2006-07-19", ""),
    ("PLZL", "Полюс", "Золотодобыча", "2006-05-03", ""),
    ("YNDX", "Яндекс", "IT", "2014-06-04", "restructured 2023-2024"),
    ("TCSG", "Т-Банк (ТКС Холдинг)", "Финансы", "2019-10-25", "ранее TCS Group"),
    ("MGNT", "Магнит", "Ритейл", "2006-04-18", ""),
    ("SNGS", "Сургутнефтегаз", "Нефть и газ", "1997-01-01", ""),
    ("CHMF", "Северсталь", "Металлургия", "2005-11-07", ""),
    ("ALRS", "АЛРОСА", "Горная добыча", "2013-10-28", ""),
    ("MOEX", "Московская биржа", "Финансы", "2013-02-15", ""),
    ("MTSS", "МТС", "Телеком", "2003-07-01", ""),
    ("VTBR", "ВТБ", "Финансы", "2007-05-29", ""),
    ("NLMK", "НЛМК", "Металлургия", "2005-12-01", ""),
    ("PHOR", "ФосАгро", "Химия", "2011-07-14", ""),
    ("TATN", "Татнефть", "Нефть и газ", "1997-01-01", ""),
    ("PIKK", "ПИК", "Строительство", "2007-06-06", ""),
    ("POLY", "Полиметалл", "Золотодобыча", "2013-06-12", "delisted 2023"),
    ("IRAO", "Интер РАО", "Электроэнергетика", "2009-01-01", ""),
    ("RUAL", "РУСАЛ", "Металлургия", "2015-01-09", "листинг на MOEX"),
    ("MAGN", "ММК", "Металлургия", "2005-04-18", ""),
    ("AFKS", "АФК Система", "Холдинг", "2005-02-11", ""),
    ("FIVE", "X5 Group", "Ритейл", "2024-04-09", "редомициляция на MOEX"),
    ("RTKM", "Ростелеком", "Телеком", "2003-01-01", ""),
    ("FEES", "ФСК-Россети", "Электроэнергетика", "2008-07-01", ""),
    ("FLOT", "Совкомфлот", "Транспорт", "2020-10-07", "IPO 2020"),
    ("OZON", "Ozon", "E-commerce", "2020-11-24", "IPO 2020"),
]


def download_ticker(session: requests.Session, ticker: str) -> pd.DataFrame:
    """Загружает дневные OHLCV-данные по одному тикеру с MOEX ISS API."""
    raw = apimoex.get_board_history(
        session,
        security=ticker,
        start=START_DATE,
        end=END_DATE,
        columns=MOEX_COLUMNS,
        board="TQBR",
    )
    df = pd.DataFrame(raw)
    if df.empty:
        print(f"  [!] {ticker}: пустой ответ, пропускаем")
        return df
    df.rename(columns=RENAME_MAP, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    return df


def save_instruments(path: Path):
    """Сохраняем справочник тикеров в CSV."""
    cols = ["ticker", "short_name", "sector", "ipo_date_moex", "notes"]
    df = pd.DataFrame(INSTRUMENTS, columns=cols)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"Справочник инструментов: {path}  ({len(df)} строк)")


def save_trading_calendar(sber_dates: pd.Series, path: Path):
    """Формируем торговый календарь из дат, в которые торговался SBER."""
    cal = pd.DataFrame({
        "date": sber_dates.sort_values().unique(),
        "is_trading_day": True,
    })
    cal["date"] = pd.to_datetime(cal["date"])
    cal.to_parquet(path, index=False)
    print(f"Торговый календарь: {path}  ({len(cal)} торговых дней)")


def main():
    print(f"Загрузка данных по {len(TICKERS_30)} тикерам за {START_DATE} .. {END_DATE}")
    print(f"Доска: TQBR (основной режим торгов)\n")

    frames = []

    with requests.Session() as session:
        # таймаут на соединение и чтение
        session.timeout = 600

        for i, ticker in enumerate(TICKERS_30, start=1):
            print(f"[{i:2d}/{len(TICKERS_30)}] {ticker} ... ", end="", flush=True)
            try:
                df = download_ticker(session, ticker)
                if not df.empty:
                    frames.append(df)
                    date_min = df["date"].min().strftime("%Y-%m-%d")
                    date_max = df["date"].max().strftime("%Y-%m-%d")
                    print(f"{len(df)} строк, {date_min} .. {date_max}")
                else:
                    print("нет данных")
            except Exception as e:
                print(f"ошибка: {e}")

            # пауза между запросами, чтобы не нарваться на rate limit
            if i < len(TICKERS_30):
                time.sleep(0.3)

    if not frames:
        print("Не удалось загрузить ни одного тикера, завершаем.")
        return

    # склеиваем все в один DataFrame
    all_data = pd.concat(frames, ignore_index=True)
    all_data.sort_values(["ticker", "date"], inplace=True)
    all_data.reset_index(drop=True, inplace=True)

    # сохраняем OHLCV
    ohlcv_path = RAW_DIR / "ohlcv_daily.parquet"
    all_data.to_parquet(ohlcv_path, index=False)
    print(f"\nOHLCV-данные: {ohlcv_path}  ({len(all_data)} строк)")

    # справочник инструментов
    save_instruments(META_DIR / "instruments.csv")

    # торговый календарь — на базе SBER (самый ликвидный, торгуется каждый день)
    sber_mask = all_data["ticker"] == "SBER"
    if sber_mask.any():
        save_trading_calendar(all_data.loc[sber_mask, "date"], META_DIR / "trading_calendar.parquet")
    else:
        print("[!] SBER не найден, торговый календарь не создан")

    # итоговая сводка
    print("\n--- Итого ---")
    summary = all_data.groupby("ticker")["date"].agg(["count", "min", "max"])
    summary.columns = ["rows", "first_date", "last_date"]
    print(summary.to_string())
    print(f"\nВсего строк: {len(all_data)}")
    print(f"Дат (уникальных): {all_data['date'].nunique()}")
    print(f"Период: {all_data['date'].min().strftime('%Y-%m-%d')} .. {all_data['date'].max().strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()
