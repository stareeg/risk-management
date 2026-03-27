"""
Поиск корпоративных действий (сплитов) по загруженным OHLCV-данным.
Проверяем аномальные дневные доходности (|return| > 40%), которые могут указывать на сплит.
Результат: data/meta/corporate_actions.csv
"""

import time
import sys
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
META_DIR = PROJECT_ROOT / "data" / "meta"

META_DIR.mkdir(parents=True, exist_ok=True)

OHLCV_PATH = RAW_DIR / "ohlcv_daily.parquet"
OUTPUT_PATH = META_DIR / "corporate_actions.csv"

# порог для определения аномальной доходности (потенциальный сплит)
RETURN_THRESHOLD = 0.40

# 30 тикеров из нашего портфеля
TICKERS_30 = [
    "SBER", "GAZP", "LKOH", "GMKN", "NVTK",
    "ROSN", "PLZL", "YNDX", "TCSG", "MGNT",
    "SNGS", "CHMF", "ALRS", "MOEX", "MTSS",
    "VTBR", "NLMK", "PHOR", "TATN", "PIKK",
    "POLY", "IRAO", "RUAL", "MAGN", "AFKS",
    "FIVE", "RTKM", "FEES", "FLOT", "OZON",
]

# справочник известных сплитов российских акций (2015-2025)
# используем для уточнения ratio, когда автодетект дает близкое, но неточное значение
# (цена в день сплита уже отражает рыночное движение, поэтому ratio из цен приблизительный)
KNOWN_SPLITS_REFERENCE = {
    # (ticker, approx_date_range): (true_ratio, action_type)
    ("GMKN", "2024-04"): (100, "split"),       # сплит 1:100 в апреле 2024
    ("PLZL", "2025-03"): (10, "split"),         # сплит 1:10 в марте 2025
    ("VTBR", "2024-07"): (5000, "reverse_split"),  # консолидация 5000:1 в июле 2024
    # TRNFP 1:100 в феврале 2020 — этого тикера нет в нашем списке
}


def wait_for_data(path: Path, max_retries: int = 30, interval: int = 10) -> bool:
    """Ждем, пока файл появится (скрипт 01 может еще работать)."""
    for attempt in range(1, max_retries + 1):
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            print(f"Файл найден: {path} ({size_mb:.1f} MB)")
            return True
        print(f"Ожидание данных... попытка {attempt}/{max_retries} "
              f"(файл: {path.name})")
        time.sleep(interval)

    print(f"Файл {path} не найден после {max_retries} попыток. "
          f"Сначала запустите 01_download_stocks.py")
    return False


def detect_splits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ищем потенциальные сплиты по аномальным дневным доходностям.
    Считаем доходность относительно последней ненулевой цены закрытия,
    потому что перед сплитом часто бывает пауза в торгах (NaN).
    """
    results = []

    for ticker in TICKERS_30:
        ticker_data = df[df["ticker"] == ticker].copy()
        if ticker_data.empty:
            continue

        ticker_data = ticker_data.sort_values("date").reset_index(drop=True)
        price_col = "close"

        # доходность считаем через pct_change, но NaN пропускаем
        # (то есть сравниваем с последней доступной ценой)
        prices = ticker_data[price_col].copy()
        prices_filled = prices.ffill()
        returns = prices / prices_filled.shift(1) - 1

        # отбираем только строки с реальной (не NaN) ценой и аномальной доходностью
        mask = prices.notna() & (returns.abs() > RETURN_THRESHOLD)
        anomaly_indices = ticker_data.index[mask]

        for i in anomaly_indices:
            row = ticker_data.iloc[i]
            ret = returns.iloc[i]
            current_price = row[price_col]

            # находим предыдущую ненулевую цену
            prev_prices = prices.iloc[:i].dropna()
            prev_price = prev_prices.iloc[-1] if len(prev_prices) > 0 else None

            if prev_price is None or prev_price == 0:
                continue

            actual_ratio_down = prev_price / current_price  # для прямого сплита
            actual_ratio_up = current_price / prev_price    # для обратного

            # определяем тип аномалии
            if ret < -RETURN_THRESHOLD and actual_ratio_down >= 2:
                # возможный прямой сплит (цена упала в N раз)
                nearest = round(actual_ratio_down)
                # допуск 10% — сплит может не быть точно кратным из-за движения рынка
                if nearest >= 2 and abs(actual_ratio_down - nearest) / nearest < 0.10:
                    action = "split"
                    ratio = nearest
                    desc = (f"Потенциальный сплит 1:{ratio}, "
                            f"цена {prev_price:.2f} -> {current_price:.2f}")
                else:
                    action = "anomaly_drop"
                    ratio = 1
                    desc = f"Аномальное падение {ret:.1%}, цена {prev_price:.2f} -> {current_price:.2f}"
            elif ret > RETURN_THRESHOLD and actual_ratio_up >= 2:
                # возможный обратный сплит / консолидация (цена выросла в N раз)
                nearest = round(actual_ratio_up)
                if nearest >= 2 and abs(actual_ratio_up - nearest) / nearest < 0.10:
                    action = "reverse_split"
                    ratio = nearest
                    desc = (f"Потенциальный обратный сплит (консолидация) {ratio}:1, "
                            f"цена {prev_price:.4f} -> {current_price:.2f}")
                else:
                    action = "anomaly_rise"
                    ratio = 1
                    desc = f"Аномальный рост {ret:.1%}, цена {prev_price:.2f} -> {current_price:.2f}"
            else:
                action = "anomaly"
                ratio = 1
                desc = f"Аномальная доходность {ret:.1%}"

            results.append({
                "date": row["date"],
                "ticker": ticker,
                "action_type": action,
                "ratio": ratio,
                "return_pct": ret,
                "price_before": prev_price,
                "price_after": current_price,
                "description": desc,
            })

    return pd.DataFrame(results)


def print_anomaly_context(df: pd.DataFrame, ticker: str, anomaly_date, window: int = 5):
    """Печатаем контекст вокруг аномальной даты для ручной проверки."""
    ticker_data = df[df["ticker"] == ticker].sort_values("date").reset_index(drop=True)
    idx = ticker_data.index[ticker_data["date"] == anomaly_date]

    if len(idx) == 0:
        return

    i = idx[0]
    start = max(0, i - window)
    end = min(len(ticker_data), i + window + 1)
    context = ticker_data.iloc[start:end][["date", "ticker", "open", "high", "low", "close", "volume"]]

    print(f"\n  Контекст для {ticker} вокруг {anomaly_date.strftime('%Y-%m-%d')}:")
    for _, r in context.iterrows():
        marker = " <<<" if r["date"] == anomaly_date else ""
        # если цена NaN — печатаем прочерки
        if pd.isna(r["close"]):
            print(f"    {r['date'].strftime('%Y-%m-%d')}  "
                  f"O={'—':>10s}  H={'—':>10s}  L={'—':>10s}  C={'—':>10s}  "
                  f"V={r['volume']:>12.0f}{marker}")
        else:
            print(f"    {r['date'].strftime('%Y-%m-%d')}  "
                  f"O={r['open']:>10.2f}  H={r['high']:>10.2f}  "
                  f"L={r['low']:>10.2f}  C={r['close']:>10.2f}  "
                  f"V={r['volume']:>12.0f}{marker}")


def refine_ratio(ticker: str, anomaly_date, detected_ratio: int, action_type: str) -> int:
    """
    Уточняем ratio по справочнику известных сплитов.
    Из-за рыночного движения в день сплита автодетект может дать неточное значение
    (например 98 вместо 100 для GMKN).
    """
    date_prefix = anomaly_date.strftime("%Y-%m")
    key = (ticker, date_prefix)

    if key in KNOWN_SPLITS_REFERENCE:
        true_ratio, expected_action = KNOWN_SPLITS_REFERENCE[key]
        if expected_action == action_type:
            # проверяем, что детектированный ratio «в районе» правильного
            tolerance = 0.20  # 20% допуск
            if abs(detected_ratio - true_ratio) / true_ratio < tolerance:
                print(f"    Уточнение ratio для {ticker}: {detected_ratio} -> {true_ratio} "
                      f"(по справочнику)")
                return true_ratio
    return detected_ratio


def classify_anomalies(anomalies_df: pd.DataFrame, ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Отделяем настоящие сплиты от рыночной волатильности.
    Критерии подтвержденного сплита:
    - Кратное изменение цены (2x, 5x, 10x, 100x и т.д.)
    - Цена после «скачка» остается на новом уровне (не возвращается обратно)
    - Для крупных российских акций за 2015-2025 известные сплиты:
      - GMKN 1:100 (апрель 2024)
      - PLZL 1:10 (март 2025)
      - VTBR консолидация 5000:1 (июль 2024)
    """
    confirmed = []

    for _, row in anomalies_df.iterrows():
        ticker = row["ticker"]
        action = row["action_type"]
        ratio = row["ratio"]
        anomaly_date = row["date"]

        if action not in ("split", "reverse_split"):
            # обычные аномалии (типа падения VTBR 24.02.2022) пропускаем
            continue

        # дополнительная проверка: цена после сплита остается на новом уровне
        # (смотрим 5 торговых дней после события)
        ticker_data = ohlcv[ohlcv["ticker"] == ticker].sort_values("date")
        post_data = ticker_data[ticker_data["date"] > anomaly_date].head(5)
        post_prices = post_data["close"].dropna()

        if len(post_prices) == 0:
            continue

        avg_post = post_prices.mean()
        price_after = row["price_after"]

        # если средняя цена после близка к цене в день события --
        # значит цена осталась на новом уровне (это сплит, а не отскок)
        deviation = abs(avg_post - price_after) / price_after
        if deviation < 0.15:
            # уточняем ratio по справочнику известных сплитов
            ratio = refine_ratio(ticker, anomaly_date, ratio, action)

            if action == "split":
                desc = (f"Сплит 1:{ratio}, цена {row['price_before']:.2f} -> "
                        f"{row['price_after']:.2f}")
            else:
                desc = (f"Консолидация (обратный сплит) {ratio}:1, цена "
                        f"{row['price_before']:.4f} -> {row['price_after']:.2f}")

            confirmed.append({
                "date": anomaly_date,
                "ticker": ticker,
                "action_type": action,
                "ratio": ratio,
                "description": desc,
            })

    return pd.DataFrame(confirmed)


def main():
    print("=" * 70)
    print("Скрипт 03: Поиск корпоративных действий (сплитов)")
    print("=" * 70)

    # ждем, пока появятся данные от скрипта 01
    if not wait_for_data(OHLCV_PATH):
        sys.exit(1)

    print("\nЗагрузка OHLCV данных...")
    df = pd.read_parquet(OHLCV_PATH)
    df["date"] = pd.to_datetime(df["date"])
    print(f"Загружено {len(df)} строк, {df['ticker'].nunique()} тикеров")

    # проверяем, что все 30 тикеров на месте
    loaded_tickers = set(df["ticker"].unique())
    missing = set(TICKERS_30) - loaded_tickers
    if missing:
        print(f"Внимание: отсутствуют тикеры: {missing}")

    # ищем аномальные доходности
    print(f"\nПоиск аномальных дневных доходностей (|return| > {RETURN_THRESHOLD:.0%})...")
    anomalies = detect_splits(df)

    if anomalies.empty:
        print("\nАномальных доходностей не найдено.")
        print("Среди наших 30 тикеров сплитов за 2015-2025 не обнаружено.")

        result = pd.DataFrame(columns=["date", "ticker", "action_type", "ratio", "description"])
        result.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print(f"\nФайл создан (пустой): {OUTPUT_PATH}")
        return

    # нашли аномалии — показываем
    print(f"\nОбнаружено {len(anomalies)} аномальных дней:")
    for _, row in anomalies.iterrows():
        print(f"  {row['date'].strftime('%Y-%m-%d')}  {row['ticker']:6s}  "
              f"return={row['return_pct']:+.1%}  "
              f"({row['description']})")
        print_anomaly_context(df, row["ticker"], row["date"])

    # классифицируем: настоящие сплиты vs просто волатильные дни
    confirmed = classify_anomalies(anomalies, df)

    if confirmed.empty:
        print("\nПосле анализа: подтвержденных сплитов не обнаружено.")
        print("Все найденные аномалии — это просто дни с высокой волатильностью.")

        result = pd.DataFrame(columns=["date", "ticker", "action_type", "ratio", "description"])
        result.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print(f"\nФайл создан (без сплитов): {OUTPUT_PATH}")
    else:
        print(f"\nПодтвержденные корпоративные действия ({len(confirmed)}):")
        for _, row in confirmed.iterrows():
            print(f"  {row['date'].strftime('%Y-%m-%d')}  {row['ticker']:6s}  "
                  f"{row['action_type']}  ratio={row['ratio']}  {row['description']}")

        # сохраняем
        out = confirmed.copy()
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        print(f"\nФайл сохранен: {OUTPUT_PATH}")

    # в любом случае сохраняем лог всех аномалий для справки
    anomaly_log = anomalies[["date", "ticker", "action_type", "return_pct",
                              "price_before", "price_after", "description"]].copy()
    anomaly_log["date"] = anomaly_log["date"].dt.strftime("%Y-%m-%d")
    anomaly_log_path = META_DIR / "anomalous_returns_log.csv"
    anomaly_log.to_csv(anomaly_log_path, index=False, encoding="utf-8-sig")
    print(f"Лог аномалий сохранен для справки: {anomaly_log_path}")

    print("\nГотово.")


if __name__ == "__main__":
    main()
