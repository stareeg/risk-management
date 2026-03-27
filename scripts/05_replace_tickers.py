"""
Замена 6 тикеров с неполной историей на 6 новых.
Удаляем: YNDX, POLY, TCSG, FIVE, FLOT, OZON
Добавляем: AFLT, HYDR, CBOM, SNGSP, MSNG, LENT
Пересобираем все датасеты (ohlcv, instruments, calendar, corporate_actions, prices_adjusted).
"""

import time
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import apimoex

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
META_DIR = PROJECT_ROOT / "data" / "meta"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

for d in [RAW_DIR, META_DIR, PROCESSED_DIR]:
    d.mkdir(parents=True, exist_ok=True)

START_DATE = "2015-01-01"
END_DATE = "2025-12-31"

# тикеры на удаление и добавление
OLD_TICKERS = {"YNDX", "POLY", "TCSG", "FIVE", "FLOT", "OZON"}
NEW_TICKERS = ["AFLT", "HYDR", "CBOM", "SNGSP", "MSNG", "LENT"]

# колонки MOEX ISS API
MOEX_COLUMNS = (
    "TRADEDATE", "SECID",
    "OPEN", "HIGH", "LOW", "CLOSE",
    "LEGALCLOSEPRICE", "WAPRICE",
    "VOLUME", "VALUE", "NUMTRADES",
)

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

# метаданные новых тикеров
NEW_INSTRUMENTS = [
    ("AFLT", "Аэрофлот", "Транспорт", "2003-01-01", ""),
    ("HYDR", "РусГидро", "Электроэнергетика", "2008-01-01", ""),
    ("CBOM", "МКБ", "Финансы", "2015-07-02", ""),
    ("SNGSP", "Сургутнефтегаз-п", "Нефть и газ", "1997-01-01", "привилегированные"),
    ("MSNG", "Мосэнерго", "Электроэнергетика", "2005-01-01", ""),
    ("LENT", "Лента", "Ритейл", "2014-02-28", ""),
]

# порог аномальной доходности для детекта сплитов
RETURN_THRESHOLD = 0.40


def download_ticker(session: requests.Session, ticker: str) -> pd.DataFrame:
    """Загрузка дневных OHLCV по одному тикеру. Все на TQBR, включая привилегированные."""
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
        print(f"  [!] {ticker}: пустой ответ")
        return df
    df.rename(columns=RENAME_MAP, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    return df


def step1_download_new_tickers():
    """Шаг 1: скачиваем OHLCV для 6 новых тикеров."""
    print("Шаг 1: загрузка данных для новых тикеров")
    print(f"Тикеры: {', '.join(NEW_TICKERS)}")
    print(f"Период: {START_DATE} .. {END_DATE}\n")

    frames = []
    with requests.Session() as session:
        session.timeout = 600
        for i, ticker in enumerate(NEW_TICKERS, start=1):
            print(f"  [{i}/{len(NEW_TICKERS)}] {ticker} ... ", end="", flush=True)
            try:
                df = download_ticker(session, ticker)
                if not df.empty:
                    frames.append(df)
                    d_min = df["date"].min().strftime("%Y-%m-%d")
                    d_max = df["date"].max().strftime("%Y-%m-%d")
                    print(f"{len(df)} строк, {d_min} .. {d_max}")
                else:
                    print("нет данных")
            except Exception as e:
                print(f"ошибка: {e}")

            if i < len(NEW_TICKERS):
                time.sleep(0.3)

    if not frames:
        raise RuntimeError("Не удалось загрузить ни одного нового тикера")

    new_data = pd.concat(frames, ignore_index=True)
    return new_data


def step2_verify_coverage(new_data: pd.DataFrame):
    """Шаг 2: проверяем, что у каждого тикера есть данные с 2015 по 2025."""
    print("\nШаг 2: проверка покрытия по датам")
    for ticker in NEW_TICKERS:
        sub = new_data[new_data["ticker"] == ticker]
        if sub.empty:
            print(f"  {ticker}: НЕТ ДАННЫХ")
            continue
        d_min = sub["date"].min()
        d_max = sub["date"].max()
        print(f"  {ticker}: {len(sub)} строк, "
              f"{d_min.strftime('%Y-%m-%d')} .. {d_max.strftime('%Y-%m-%d')}")
        # предупреждение, если первый год не 2015
        if d_min.year > 2015:
            print(f"    (данные начинаются позже 2015 -- возможно, IPO было позже)")


def step3_update_ohlcv(new_data: pd.DataFrame):
    """Шаг 3: обновляем ohlcv_daily.parquet -- убираем старые, добавляем новые."""
    print("\nШаг 3: обновление ohlcv_daily.parquet")

    ohlcv_path = RAW_DIR / "ohlcv_daily.parquet"
    ohlcv = pd.read_parquet(ohlcv_path)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    n_before = len(ohlcv)
    tickers_before = set(ohlcv["ticker"].unique())
    print(f"  До: {n_before} строк, {len(tickers_before)} тикеров")

    # удаляем старые
    ohlcv = ohlcv[~ohlcv["ticker"].isin(OLD_TICKERS)]
    n_after_remove = len(ohlcv)
    print(f"  Удалено {n_before - n_after_remove} строк ({', '.join(sorted(OLD_TICKERS))})")

    # добавляем новые (убедимся, что колонки совпадают)
    new_data = new_data[ohlcv.columns].copy()
    ohlcv = pd.concat([ohlcv, new_data], ignore_index=True)
    ohlcv.sort_values(["ticker", "date"], inplace=True)
    ohlcv.reset_index(drop=True, inplace=True)

    print(f"  После: {len(ohlcv)} строк, {ohlcv['ticker'].nunique()} тикеров")
    print(f"  Тикеры: {sorted(ohlcv['ticker'].unique())}")

    ohlcv.to_parquet(ohlcv_path, index=False)
    print(f"  Сохранено: {ohlcv_path}")
    return ohlcv


def step4_update_instruments():
    """Шаг 4: обновляем instruments.csv."""
    print("\nШаг 4: обновление instruments.csv")

    inst_path = META_DIR / "instruments.csv"
    inst = pd.read_csv(inst_path)
    print(f"  До: {len(inst)} строк")

    # удаляем старые
    inst = inst[~inst["ticker"].isin(OLD_TICKERS)]

    # добавляем новые
    new_rows = pd.DataFrame(
        NEW_INSTRUMENTS,
        columns=["ticker", "short_name", "sector", "ipo_date_moex", "notes"],
    )
    inst = pd.concat([inst, new_rows], ignore_index=True)

    print(f"  После: {len(inst)} строк")
    inst.to_csv(inst_path, index=False, encoding="utf-8-sig")
    print(f"  Сохранено: {inst_path}")


def step5_rebuild_calendar(ohlcv: pd.DataFrame):
    """Шаг 5: пересобираем торговый календарь из дат SBER."""
    print("\nШаг 5: пересборка торгового календаря")

    sber = ohlcv[ohlcv["ticker"] == "SBER"]
    cal = pd.DataFrame({
        "date": sber["date"].sort_values().unique(),
        "is_trading_day": True,
    })
    cal["date"] = pd.to_datetime(cal["date"])

    cal_path = META_DIR / "trading_calendar.parquet"
    cal.to_parquet(cal_path, index=False)
    print(f"  {len(cal)} торговых дней, сохранено: {cal_path}")


def step6_detect_splits(ohlcv: pd.DataFrame):
    """Шаг 6: ищем аномальные доходности у новых тикеров (потенциальные сплиты)."""
    print("\nШаг 6: поиск сплитов для новых тикеров")

    anomalies_found = []

    for ticker in NEW_TICKERS:
        sub = ohlcv[ohlcv["ticker"] == ticker].copy()
        if sub.empty:
            continue

        sub = sub.sort_values("date").reset_index(drop=True)
        prices = sub["close"].copy()
        prices_filled = prices.ffill()
        returns = prices / prices_filled.shift(1) - 1

        mask = prices.notna() & (returns.abs() > RETURN_THRESHOLD)
        anom_idx = sub.index[mask]

        if len(anom_idx) == 0:
            print(f"  {ticker}: аномалий не найдено")
            continue

        for i in anom_idx:
            row = sub.iloc[i]
            ret = returns.iloc[i]
            curr_price = row["close"]

            prev_prices = prices.iloc[:i].dropna()
            if len(prev_prices) == 0:
                continue
            prev_price = prev_prices.iloc[-1]
            if prev_price == 0:
                continue

            print(f"  {ticker}: {row['date'].strftime('%Y-%m-%d')}, "
                  f"return={ret:+.1%}, цена {prev_price:.2f} -> {curr_price:.2f}")

            # контекст: 3 строки до и после
            start_ctx = max(0, i - 3)
            end_ctx = min(len(sub), i + 4)
            ctx = sub.iloc[start_ctx:end_ctx][["date", "ticker", "close", "volume"]]
            for _, cr in ctx.iterrows():
                marker = " <<<" if cr["date"] == row["date"] else ""
                print(f"    {cr['date'].strftime('%Y-%m-%d')}  "
                      f"C={cr['close']:>10.2f}  V={cr['volume']:>12.0f}{marker}")

            # проверяем, похоже ли на сплит
            ratio_down = prev_price / curr_price
            ratio_up = curr_price / prev_price

            if ret < -RETURN_THRESHOLD and ratio_down >= 2:
                nearest = round(ratio_down)
                if nearest >= 2 and abs(ratio_down - nearest) / nearest < 0.10:
                    # проверяем, что цена осталась на новом уровне (5 дней после)
                    post = sub.iloc[i+1:i+6]["close"].dropna()
                    if len(post) > 0:
                        deviation = abs(post.mean() - curr_price) / curr_price
                        if deviation < 0.15:
                            desc = f"Сплит 1:{nearest}, цена {prev_price:.2f} -> {curr_price:.2f}"
                            anomalies_found.append({
                                "date": row["date"].strftime("%Y-%m-%d"),
                                "ticker": ticker,
                                "action_type": "split",
                                "ratio": nearest,
                                "description": desc,
                            })
                            print(f"    -> подтвержденный сплит 1:{nearest}")

            elif ret > RETURN_THRESHOLD and ratio_up >= 2:
                nearest = round(ratio_up)
                if nearest >= 2 and abs(ratio_up - nearest) / nearest < 0.10:
                    post = sub.iloc[i+1:i+6]["close"].dropna()
                    if len(post) > 0:
                        deviation = abs(post.mean() - curr_price) / curr_price
                        if deviation < 0.15:
                            desc = (f"Консолидация (обратный сплит) {nearest}:1, "
                                    f"цена {prev_price:.4f} -> {curr_price:.2f}")
                            anomalies_found.append({
                                "date": row["date"].strftime("%Y-%m-%d"),
                                "ticker": ticker,
                                "action_type": "reverse_split",
                                "ratio": nearest,
                                "description": desc,
                            })
                            print(f"    -> подтвержденный обратный сплит {nearest}:1")

    return anomalies_found


def step6_update_corporate_actions(new_splits: list):
    """Обновляем corporate_actions.csv: оставляем GMKN, PLZL, VTBR; убираем старые тикеры."""
    print("\nОбновление corporate_actions.csv")

    ca_path = META_DIR / "corporate_actions.csv"
    ca = pd.read_csv(ca_path)

    # убираем записи для старых тикеров (если вдруг есть)
    ca = ca[~ca["ticker"].isin(OLD_TICKERS)]
    print(f"  Существующие записи (после удаления старых тикеров): {len(ca)}")
    for _, row in ca.iterrows():
        print(f"    {row['date']}  {row['ticker']}  {row['action_type']}  ratio={row['ratio']}")

    # добавляем новые сплиты, если нашлись
    if new_splits:
        new_df = pd.DataFrame(new_splits)
        ca = pd.concat([ca, new_df], ignore_index=True)
        print(f"  Добавлено новых записей: {len(new_splits)}")
    else:
        print("  Новых сплитов не обнаружено")

    ca.to_csv(ca_path, index=False, encoding="utf-8-sig")
    print(f"  Сохранено: {ca_path} ({len(ca)} записей)")


def step7_rebuild_prices_adjusted():
    """Шаг 7: пересобираем prices_adjusted.parquet с учетом сплитов."""
    print("\nШаг 7: пересборка prices_adjusted.parquet")

    # загружаем обновленные данные
    ohlcv = pd.read_parquet(RAW_DIR / "ohlcv_daily.parquet")
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    ca_path = META_DIR / "corporate_actions.csv"
    corp_actions = pd.read_csv(ca_path)

    # backward-корректировка на сплиты
    if not corp_actions.empty:
        corp_actions["date"] = pd.to_datetime(corp_actions["date"])
        price_cols = [c for c in ["open", "high", "low", "close", "close_official", "waprice"]
                      if c in ohlcv.columns]

        for _, action in corp_actions.iterrows():
            if action["action_type"] not in ("split", "reverse_split"):
                continue

            ticker = action["ticker"]
            split_date = pd.to_datetime(action["date"])
            ratio = action["ratio"]
            mask = (ohlcv["ticker"] == ticker) & (ohlcv["date"] < split_date)
            n_adj = mask.sum()

            if action["action_type"] == "split":
                for col in price_cols:
                    ohlcv.loc[mask, col] = ohlcv.loc[mask, col] / ratio
                if "volume" in ohlcv.columns:
                    ohlcv.loc[mask, "volume"] = ohlcv.loc[mask, "volume"] * ratio
                print(f"  {ticker}: сплит 1:{ratio}, скорректировано {n_adj} строк")

            elif action["action_type"] == "reverse_split":
                for col in price_cols:
                    ohlcv.loc[mask, col] = ohlcv.loc[mask, col] * ratio
                if "volume" in ohlcv.columns:
                    ohlcv.loc[mask, "volume"] = ohlcv.loc[mask, "volume"] / ratio
                print(f"  {ticker}: обратный сплит {ratio}:1, скорректировано {n_adj} строк")
    else:
        print("  Сплитов нет, корректировка не нужна")

    # close_adj: приоритет у close_official, если не null/zero
    if "close_official" in ohlcv.columns:
        # если close_official == 0, считаем его пропуском
        valid_official = ohlcv["close_official"].notna() & (ohlcv["close_official"] != 0)
        ohlcv["close_adj"] = ohlcv["close"].copy()
        ohlcv.loc[valid_official, "close_adj"] = ohlcv.loc[valid_official, "close_official"]
    else:
        ohlcv["close_adj"] = ohlcv["close"]

    result = ohlcv[["date", "ticker", "close_adj"]].copy()
    result = result.sort_values(["ticker", "date"]).reset_index(drop=True)

    # forward fill с лимитом 5 внутри каждого тикера
    n_nan_before = result["close_adj"].isna().sum()
    if n_nan_before > 0:
        result["close_adj"] = result.groupby("ticker")["close_adj"].transform(
            lambda x: x.ffill(limit=5)
        )
        n_nan_after = result["close_adj"].isna().sum()
        print(f"  Forward fill: {n_nan_before} NaN до, {n_nan_after} NaN после")
    else:
        print(f"  NaN в close_adj: нет")

    out_path = PROCESSED_DIR / "prices_adjusted.parquet"
    result.to_parquet(out_path, index=False)
    print(f"  Сохранено: {out_path} ({len(result)} строк)")
    return result


def print_summary():
    """Итоговая сводка по всем 30 тикерам."""
    print("\n" + "-" * 70)
    print("ИТОГО: все 30 тикеров")
    print("-" * 70)

    ohlcv = pd.read_parquet(RAW_DIR / "ohlcv_daily.parquet")
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    summary = ohlcv.groupby("ticker")["date"].agg(["count", "min", "max"])
    summary.columns = ["rows", "first_date", "last_date"]
    summary = summary.sort_values("rows", ascending=False)

    print(f"\n{'Тикер':<8} {'Строк':>6} {'Первая дата':>12} {'Последняя дата':>15}")
    print("-" * 45)
    for ticker, row in summary.iterrows():
        print(f"{ticker:<8} {row['rows']:>6} "
              f"{row['first_date'].strftime('%Y-%m-%d'):>12} "
              f"{row['last_date'].strftime('%Y-%m-%d'):>15}")

    print(f"\nВсего строк: {len(ohlcv)}")
    print(f"Тикеров: {ohlcv['ticker'].nunique()}")
    print(f"Период: {ohlcv['date'].min().strftime('%Y-%m-%d')} .. "
          f"{ohlcv['date'].max().strftime('%Y-%m-%d')}")


def main():
    print("Замена 6 тикеров с неполной историей")
    print(f"Удаляем: {', '.join(sorted(OLD_TICKERS))}")
    print(f"Добавляем: {', '.join(NEW_TICKERS)}")
    print()

    # 1. загрузка новых тикеров
    new_data = step1_download_new_tickers()

    # 2. проверка покрытия
    step2_verify_coverage(new_data)

    # 3. обновление OHLCV
    ohlcv = step3_update_ohlcv(new_data)

    # 4. обновление instruments.csv
    step4_update_instruments()

    # 5. пересборка торгового календаря
    step5_rebuild_calendar(ohlcv)

    # 6. детект сплитов для новых тикеров
    new_splits = step6_detect_splits(ohlcv)
    step6_update_corporate_actions(new_splits)

    # 7. пересборка prices_adjusted
    step7_rebuild_prices_adjusted()

    # итоговая сводка
    print_summary()

    print("\nГотово, все датасеты обновлены.")


if __name__ == "__main__":
    main()
