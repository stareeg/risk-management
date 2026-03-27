"""
Замена LENT на MVID в датасетах.
LENT (Лента) -- данные только с декабря 2021, слишком короткий ряд.
Вместо неё берем MVID (М.Видео), которая торгуется с 2007 года.
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

# колонки MOEX ISS API -- те же, что и в остальных скриптах
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

# порог для детекта сплитов
RETURN_THRESHOLD = 0.40


def step1_download_mvid():
    """Загружаем дневные OHLCV по MVID с MOEX ISS."""
    print("Шаг 1: загрузка MVID с MOEX ISS API")
    print(f"Период: {START_DATE} .. {END_DATE}\n")

    with requests.Session() as session:
        session.timeout = 600
        raw = apimoex.get_board_history(
            session,
            security='MVID',
            start='2015-01-01',
            end='2025-12-31',
            board='TQBR',
            columns=('TRADEDATE', 'SECID', 'OPEN', 'HIGH', 'LOW', 'CLOSE',
                     'LEGALCLOSEPRICE', 'WAPRICE', 'VOLUME', 'VALUE', 'NUMTRADES'),
        )

    df = pd.DataFrame(raw)
    if df.empty:
        raise RuntimeError("MVID: пустой ответ от MOEX ISS, что-то пошло не так")

    df.rename(columns=RENAME_MAP, inplace=True)
    df["date"] = pd.to_datetime(df["date"])

    d_min = df["date"].min().strftime("%Y-%m-%d")
    d_max = df["date"].max().strftime("%Y-%m-%d")
    print(f"  MVID: {len(df)} строк, {d_min} .. {d_max}")

    return df


def step2_update_ohlcv(mvid_data: pd.DataFrame):
    """Обновляем ohlcv_daily.parquet: убираем LENT, добавляем MVID."""
    print("\nШаг 2: обновление ohlcv_daily.parquet")

    ohlcv_path = RAW_DIR / "ohlcv_daily.parquet"
    ohlcv = pd.read_parquet(ohlcv_path)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    n_before = len(ohlcv)
    tickers_before = sorted(ohlcv["ticker"].unique())
    print(f"  До: {n_before} строк, {len(tickers_before)} тикеров")

    # сколько строк LENT было
    n_lent = (ohlcv["ticker"] == "LENT").sum()
    print(f"  Строк LENT для удаления: {n_lent}")

    # удаляем LENT
    ohlcv = ohlcv[ohlcv["ticker"] != "LENT"].copy()

    # добавляем MVID (приводим колонки к тому же порядку)
    mvid_data = mvid_data[ohlcv.columns].copy()
    ohlcv = pd.concat([ohlcv, mvid_data], ignore_index=True)
    ohlcv.sort_values(["ticker", "date"], inplace=True)
    ohlcv.reset_index(drop=True, inplace=True)

    tickers_after = sorted(ohlcv["ticker"].unique())
    print(f"  После: {len(ohlcv)} строк, {len(tickers_after)} тикеров")

    # проверка, что LENT нет, а MVID есть
    assert "LENT" not in tickers_after, "LENT все ещё в данных"
    assert "MVID" in tickers_after, "MVID не попал в данные"

    ohlcv.to_parquet(ohlcv_path, index=False)
    print(f"  Сохранено: {ohlcv_path}")

    return ohlcv


def step3_update_instruments():
    """Обновляем instruments.csv: убираем LENT, добавляем MVID."""
    print("\nШаг 3: обновление instruments.csv")

    inst_path = META_DIR / "instruments.csv"
    inst = pd.read_csv(inst_path)
    print(f"  До: {len(inst)} строк")

    # убираем LENT
    inst = inst[inst["ticker"] != "LENT"].copy()

    # добавляем MVID
    new_row = pd.DataFrame([{
        "ticker": "MVID",
        "short_name": "М.Видео",
        "sector": "Ритейл",
        "ipo_date_moex": "2007-11-01",
        "notes": "",
    }])
    inst = pd.concat([inst, new_row], ignore_index=True)

    print(f"  После: {len(inst)} строк")
    print(f"  Тикеры: {sorted(inst['ticker'].tolist())}")

    inst.to_csv(inst_path, index=False, encoding="utf-8-sig")
    print(f"  Сохранено: {inst_path}")


def step4_check_splits(ohlcv: pd.DataFrame):
    """Ищем аномальные доходности у MVID (потенциальные сплиты)."""
    print("\nШаг 4: проверка MVID на сплиты (|return| > 40%)")

    sub = ohlcv[ohlcv["ticker"] == "MVID"].copy()
    sub = sub.sort_values("date").reset_index(drop=True)

    prices = sub["close"].copy()
    prices_filled = prices.ffill()
    returns = prices / prices_filled.shift(1) - 1

    mask = prices.notna() & (returns.abs() > RETURN_THRESHOLD)
    anom_idx = sub.index[mask]

    anomalies_found = []

    if len(anom_idx) == 0:
        print("  MVID: аномалий не найдено, сплитов нет")
        return anomalies_found

    print(f"  Найдено аномальных дней: {len(anom_idx)}")

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

        print(f"\n  MVID: {row['date'].strftime('%Y-%m-%d')}, "
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
                # проверяем, что цена осталась на новом уровне
                post = sub.iloc[i+1:i+6]["close"].dropna()
                if len(post) > 0:
                    deviation = abs(post.mean() - curr_price) / curr_price
                    if deviation < 0.15:
                        desc = f"Сплит 1:{nearest}, цена {prev_price:.2f} -> {curr_price:.2f}"
                        anomalies_found.append({
                            "date": row["date"].strftime("%Y-%m-%d"),
                            "ticker": "MVID",
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
                            "ticker": "MVID",
                            "action_type": "reverse_split",
                            "ratio": nearest,
                            "description": desc,
                        })
                        print(f"    -> подтвержденный обратный сплит {nearest}:1")

    if not anomalies_found:
        print("\n  Подтвержденных сплитов не обнаружено (аномалии -- рыночная волатильность)")

    return anomalies_found


def step4_update_corporate_actions(new_splits: list):
    """Обновляем corporate_actions.csv если нашлись сплиты MVID."""
    ca_path = META_DIR / "corporate_actions.csv"
    ca = pd.read_csv(ca_path)

    # удаляем возможные старые записи LENT (на всякий случай)
    ca = ca[ca["ticker"] != "LENT"].copy()

    if new_splits:
        print(f"\n  Добавляем {len(new_splits)} записей о сплитах MVID в corporate_actions.csv")
        new_df = pd.DataFrame(new_splits)
        ca = pd.concat([ca, new_df], ignore_index=True)
    else:
        print("\n  Сплитов MVID нет, corporate_actions.csv не меняется (только убрали LENT)")

    ca.to_csv(ca_path, index=False, encoding="utf-8-sig")
    print(f"  Сохранено: {ca_path} ({len(ca)} записей)")

    # печатаем текущее содержимое
    print("  Текущие записи:")
    for _, row in ca.iterrows():
        print(f"    {row['date']}  {row['ticker']}  {row['action_type']}  ratio={row['ratio']}")


def step5_rebuild_prices_adjusted():
    """Пересобираем prices_adjusted.parquet с учетом всех сплитов."""
    print("\nШаг 5: пересборка prices_adjusted.parquet")

    ohlcv = pd.read_parquet(RAW_DIR / "ohlcv_daily.parquet")
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    ca_path = META_DIR / "corporate_actions.csv"
    corp_actions = pd.read_csv(ca_path)

    # backward-корректировка на сплиты (все записи из corporate_actions)
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

    # close_adj: приоритет у close_official, если значение не null и не 0
    if "close_official" in ohlcv.columns:
        valid_official = ohlcv["close_official"].notna() & (ohlcv["close_official"] != 0)
        ohlcv["close_adj"] = ohlcv["close"].copy()
        ohlcv.loc[valid_official, "close_adj"] = ohlcv.loc[valid_official, "close_official"]
    else:
        ohlcv["close_adj"] = ohlcv["close"]

    result = ohlcv[["date", "ticker", "close_adj"]].copy()
    result = result.sort_values(["ticker", "date"]).reset_index(drop=True)

    # forward fill с лимитом 5 дней внутри каждого тикера
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


def step6_print_summary():
    """Итоговая сводка по всем 30 тикерам."""
    print("\n" + "-" * 70)
    print("Шаг 6: итоговая сводка -- все 30 тикеров")
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

    return ohlcv


def step7_verify_intersection(ohlcv: pd.DataFrame):
    """Проверяем пересечение дат всех 30 тикеров."""
    print("\n" + "-" * 70)
    print("Шаг 7: пересечение дат всех 30 тикеров")
    print("-" * 70)

    tickers = sorted(ohlcv["ticker"].unique())
    print(f"Тикеров: {len(tickers)}")

    # собираем множества дат для каждого тикера
    date_sets = {}
    for ticker in tickers:
        dates = set(ohlcv[ohlcv["ticker"] == ticker]["date"])
        date_sets[ticker] = dates

    # пересечение всех
    common_dates = None
    for ticker in tickers:
        if common_dates is None:
            common_dates = date_sets[ticker].copy()
        else:
            common_dates = common_dates & date_sets[ticker]

    common_dates_sorted = sorted(common_dates)
    print(f"Дат в пересечении всех {len(tickers)} тикеров: {len(common_dates_sorted)}")

    if common_dates_sorted:
        print(f"Период пересечения: {common_dates_sorted[0].strftime('%Y-%m-%d')} .. "
              f"{common_dates_sorted[-1].strftime('%Y-%m-%d')}")

    # какие тикеры сужают пересечение сильнее всего
    # (у кого меньше всего дат)
    coverage = pd.DataFrame({
        "ticker": tickers,
        "total_dates": [len(date_sets[t]) for t in tickers],
        "in_common": [len(date_sets[t] & common_dates) for t in tickers],
    })
    coverage = coverage.sort_values("total_dates")

    print(f"\nТикеры с наименьшим числом дат (ограничивают пересечение):")
    for _, row in coverage.head(5).iterrows():
        print(f"  {row['ticker']}: {row['total_dates']} дат")


def main():
    print("Замена LENT -> MVID")
    print("LENT (Лента) -- данные с декабря 2021, слишком короткий ряд")
    print("MVID (М.Видео) -- торгуется с 2007, подходит лучше")
    print()

    # 1. скачиваем MVID
    mvid_data = step1_download_mvid()

    # 2. обновляем ohlcv
    ohlcv = step2_update_ohlcv(mvid_data)

    # 3. обновляем instruments
    step3_update_instruments()

    # 4. проверяем сплиты MVID и обновляем corporate_actions
    new_splits = step4_check_splits(ohlcv)
    step4_update_corporate_actions(new_splits)

    # 5. пересобираем prices_adjusted
    step5_rebuild_prices_adjusted()

    # 6. итоговая сводка
    ohlcv = step6_print_summary()

    # 7. пересечение дат
    step7_verify_intersection(ohlcv)

    print("\nГотово, LENT заменена на MVID во всех датасетах.")


if __name__ == "__main__":
    main()
