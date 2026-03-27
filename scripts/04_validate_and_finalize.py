"""
Валидация собранных данных и формирование финального датасета.
Проверяем: дубликаты, пропуски, ценовые аномалии, торговый календарь.
Корректируем на сплиты (если есть).
Результат: data/processed/prices_adjusted.parquet
"""

import time
import sys
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
META_DIR = PROJECT_ROOT / "data" / "meta"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# пути к файлам
OHLCV_PATH = RAW_DIR / "ohlcv_daily.parquet"
BENCHMARK_PATH = RAW_DIR / "benchmark_daily.parquet"
RISK_FREE_PATH = RAW_DIR / "risk_free_rate.parquet"
CALENDAR_PATH = META_DIR / "trading_calendar.parquet"
CORP_ACTIONS_PATH = META_DIR / "corporate_actions.csv"
OUTPUT_PATH = PROCESSED_DIR / "prices_adjusted.parquet"

# период приостановки торгов на MOEX в 2022 (вторжение в Украину)
HALT_START = pd.Timestamp("2022-02-25")
HALT_END = pd.Timestamp("2022-03-24")

# 30 тикеров
TICKERS_30 = [
    "SBER", "GAZP", "LKOH", "GMKN", "NVTK",
    "ROSN", "PLZL", "YNDX", "TCSG", "MGNT",
    "SNGS", "CHMF", "ALRS", "MOEX", "MTSS",
    "VTBR", "NLMK", "PHOR", "TATN", "PIKK",
    "POLY", "IRAO", "RUAL", "MAGN", "AFKS",
    "FIVE", "RTKM", "FEES", "FLOT", "OZON",
]


def wait_for_file(path: Path, name: str, max_retries: int = 30, interval: int = 10) -> bool:
    """Ждем появления файла."""
    for attempt in range(1, max_retries + 1):
        if path.exists():
            size = path.stat().st_size
            if size > 0:
                print(f"  {name}: найден ({size / 1024:.1f} KB)")
                return True
            else:
                print(f"  {name}: файл пустой, ждем...")
        else:
            if attempt == 1:
                print(f"  {name}: ожидание...")
            elif attempt % 5 == 0:
                print(f"  {name}: попытка {attempt}/{max_retries}...")
        time.sleep(interval)
    print(f"  {name}: НЕ НАЙДЕН после {max_retries} попыток")
    return False


def load_all_data():
    """Загружаем все необходимые файлы."""
    print("Загрузка данных...")

    # OHLCV — обязательный файл
    if not wait_for_file(OHLCV_PATH, "ohlcv_daily.parquet"):
        print("Критическая ошибка: нет OHLCV данных. Запустите скрипт 01 сначала.")
        sys.exit(1)
    ohlcv = pd.read_parquet(OHLCV_PATH)
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])

    # бенчмарк
    benchmark = None
    if BENCHMARK_PATH.exists():
        benchmark = pd.read_parquet(BENCHMARK_PATH)
        benchmark["date"] = pd.to_datetime(benchmark["date"])
        print(f"  benchmark_daily.parquet: {len(benchmark)} строк")
    else:
        print("  benchmark_daily.parquet: не найден (пропускаем)")

    # безрисковая ставка
    risk_free = None
    if RISK_FREE_PATH.exists():
        risk_free = pd.read_parquet(RISK_FREE_PATH)
        if "date" in risk_free.columns:
            risk_free["date"] = pd.to_datetime(risk_free["date"])
        print(f"  risk_free_rate.parquet: {len(risk_free)} строк")
    else:
        print("  risk_free_rate.parquet: не найден (пропускаем)")

    # торговый календарь
    calendar = None
    if CALENDAR_PATH.exists():
        calendar = pd.read_parquet(CALENDAR_PATH)
        calendar["date"] = pd.to_datetime(calendar["date"])
        print(f"  trading_calendar.parquet: {len(calendar)} дней")
    else:
        print("  trading_calendar.parquet: не найден (пропускаем проверку по календарю)")

    # корпоративные действия
    corp_actions = pd.DataFrame(columns=["date", "ticker", "action_type", "ratio", "description"])
    if CORP_ACTIONS_PATH.exists():
        corp_actions = pd.read_csv(CORP_ACTIONS_PATH)
        if not corp_actions.empty:
            corp_actions["date"] = pd.to_datetime(corp_actions["date"])
        print(f"  corporate_actions.csv: {len(corp_actions)} записей")
    else:
        print("  corporate_actions.csv: не найден (считаем, что сплитов нет)")

    return ohlcv, benchmark, risk_free, calendar, corp_actions


def validate_row_counts(ohlcv: pd.DataFrame):
    """Сводка по количеству строк для каждого тикера."""
    print("\n" + "-" * 60)
    print("1. Количество торговых дней по тикерам")
    print("-" * 60)

    summary = ohlcv.groupby("ticker")["date"].agg(["count", "min", "max"])
    summary.columns = ["rows", "first_date", "last_date"]
    summary["first_date"] = summary["first_date"].dt.strftime("%Y-%m-%d")
    summary["last_date"] = summary["last_date"].dt.strftime("%Y-%m-%d")
    summary = summary.sort_values("rows", ascending=False)

    print(f"\n{'Тикер':<8} {'Строк':>6} {'Первая дата':>12} {'Последняя дата':>15}")
    print("-" * 45)
    for ticker, row in summary.iterrows():
        print(f"{ticker:<8} {row['rows']:>6} {row['first_date']:>12} {row['last_date']:>15}")

    print(f"\nВсего строк: {len(ohlcv)}")
    print(f"Тикеров: {ohlcv['ticker'].nunique()}")

    # тикеры с коротким рядом
    short_tickers = summary[summary["rows"] < 1000]
    if not short_tickers.empty:
        print(f"\nТикеры с коротким рядом (<1000 дней): "
              f"{', '.join(short_tickers.index.tolist())}")
        print("Это нормально для бумаг с IPO после 2015 (TCSG, FIVE, FLOT, OZON, POLY)")


def validate_duplicates(ohlcv: pd.DataFrame):
    """Проверяем дубликаты date+ticker."""
    print("\n" + "-" * 60)
    print("2. Проверка дубликатов (date + ticker)")
    print("-" * 60)

    dupes = ohlcv.duplicated(subset=["date", "ticker"], keep=False)
    n_dupes = dupes.sum()

    if n_dupes == 0:
        print("Дубликатов не найдено.")
    else:
        print(f"Найдено {n_dupes} дубликатов!")
        dupe_rows = ohlcv[dupes].sort_values(["ticker", "date"])
        print(dupe_rows[["date", "ticker", "close"]].head(20).to_string())
        print("Рекомендация: оставляем последнюю запись (может быть пересчет по итогам дня)")

    return n_dupes


def validate_prices(ohlcv: pd.DataFrame):
    """Проверяем корректность цен: неотрицательные, high >= low, close в диапазоне."""
    print("\n" + "-" * 60)
    print("3. Проверка корректности цен")
    print("-" * 60)

    issues = []

    # отрицательные или нулевые цены закрытия
    neg_close = ohlcv[ohlcv["close"] <= 0]
    if not neg_close.empty:
        print(f"  Отрицательные/нулевые close: {len(neg_close)} строк")
        issues.append(("negative_close", len(neg_close)))
    else:
        print("  Отрицательные/нулевые close: нет")

    # проверяем open, high, low тоже
    for col in ["open", "high", "low"]:
        if col in ohlcv.columns:
            neg = ohlcv[ohlcv[col] <= 0]
            if not neg.empty:
                print(f"  Отрицательные/нулевые {col}: {len(neg)} строк")
                issues.append((f"negative_{col}", len(neg)))
            else:
                print(f"  Отрицательные/нулевые {col}: нет")

    # high >= low
    if "high" in ohlcv.columns and "low" in ohlcv.columns:
        bad_hl = ohlcv[ohlcv["high"] < ohlcv["low"]]
        if not bad_hl.empty:
            print(f"  high < low: {len(bad_hl)} строк")
            for _, r in bad_hl.head(5).iterrows():
                print(f"    {r['date'].strftime('%Y-%m-%d')} {r['ticker']} "
                      f"H={r['high']:.2f} L={r['low']:.2f}")
            issues.append(("high_lt_low", len(bad_hl)))
        else:
            print("  high >= low: OK")

    # close в пределах [low, high]
    if "high" in ohlcv.columns and "low" in ohlcv.columns:
        # небольшой допуск на ошибки округления
        eps = 0.01
        bad_range = ohlcv[(ohlcv["close"] > ohlcv["high"] + eps) |
                          (ohlcv["close"] < ohlcv["low"] - eps)]
        if not bad_range.empty:
            print(f"  close вне [low, high]: {len(bad_range)} строк")
            for _, r in bad_range.head(5).iterrows():
                print(f"    {r['date'].strftime('%Y-%m-%d')} {r['ticker']} "
                      f"L={r['low']:.2f} C={r['close']:.2f} H={r['high']:.2f}")
            issues.append(("close_out_of_range", len(bad_range)))
        else:
            print("  close в пределах [low, high]: OK")

    return issues


def validate_calendar(ohlcv: pd.DataFrame, calendar: pd.DataFrame):
    """Сравниваем даты торгов каждого тикера с торговым календарем."""
    print("\n" + "-" * 60)
    print("4. Проверка пропущенных дат (vs торговый календарь)")
    print("-" * 60)

    if calendar is None:
        print("Торговый календарь не загружен, пропускаем.")
        return

    cal_dates = set(calendar["date"])
    print(f"Дней в торговом календаре: {len(cal_dates)}")
    print(f"Период: {calendar['date'].min().strftime('%Y-%m-%d')} .. "
          f"{calendar['date'].max().strftime('%Y-%m-%d')}")

    missing_stats = []

    for ticker in sorted(ohlcv["ticker"].unique()):
        ticker_dates = set(ohlcv[ohlcv["ticker"] == ticker]["date"])
        # даты из календаря, которых нет у этого тикера
        # (учитываем, что у бумаг с поздним IPO первые даты будут пропущены)
        ticker_first = min(ticker_dates)
        ticker_last = max(ticker_dates)

        relevant_cal = {d for d in cal_dates if ticker_first <= d <= ticker_last}
        missing = relevant_cal - ticker_dates
        extra = ticker_dates - cal_dates

        if missing:
            # фильтруем период приостановки торгов в 2022
            missing_excl_halt = {d for d in missing
                                 if not (HALT_START <= d <= HALT_END)}
            missing_in_halt = len(missing) - len(missing_excl_halt)

            missing_stats.append({
                "ticker": ticker,
                "missing_total": len(missing),
                "missing_excl_halt": len(missing_excl_halt),
                "missing_in_halt": missing_in_halt,
            })

    if missing_stats:
        ms_df = pd.DataFrame(missing_stats).sort_values("missing_total", ascending=False)
        print(f"\n{'Тикер':<8} {'Пропущено':>10} {'Без halt':>10} {'В halt':>8}")
        print("-" * 40)
        for _, row in ms_df.iterrows():
            print(f"{row['ticker']:<8} {row['missing_total']:>10} "
                  f"{row['missing_excl_halt']:>10} {row['missing_in_halt']:>8}")
    else:
        print("Пропущенных дат не обнаружено.")


def report_halt_period(ohlcv: pd.DataFrame):
    """Отчет о периоде приостановки торгов (февраль-март 2022)."""
    print("\n" + "-" * 60)
    print("5. Период приостановки торгов (февраль-март 2022)")
    print("-" * 60)

    halt_data = ohlcv[(ohlcv["date"] >= HALT_START) & (ohlcv["date"] <= HALT_END)]
    if halt_data.empty:
        print(f"Данных за {HALT_START.strftime('%Y-%m-%d')} .. {HALT_END.strftime('%Y-%m-%d')} нет.")
        print("Это ожидаемо: торги были приостановлены после начала СВО.")
    else:
        print(f"Найдено {len(halt_data)} записей в период приостановки.")
        print(f"Тикеры: {halt_data['ticker'].nunique()}")
        dates_in_halt = sorted(halt_data["date"].unique())
        print(f"Даты: {dates_in_halt[0].strftime('%Y-%m-%d')} .. "
              f"{dates_in_halt[-1].strftime('%Y-%m-%d')}")
        print("Некоторые бумаги возобновили торги раньше других.")

    # последний торговый день перед остановкой
    pre_halt = ohlcv[ohlcv["date"] < HALT_START]
    if not pre_halt.empty:
        last_day = pre_halt["date"].max()
        print(f"\nПоследний торговый день до остановки: {last_day.strftime('%Y-%m-%d')}")

    # первый торговый день после возобновления
    post_halt = ohlcv[ohlcv["date"] > HALT_END]
    if not post_halt.empty:
        first_day = post_halt["date"].min()
        print(f"Первый торговый день после возобновления: {first_day.strftime('%Y-%m-%d')}")


def validate_benchmark(benchmark: pd.DataFrame):
    """Базовая проверка данных бенчмарка."""
    print("\n" + "-" * 60)
    print("6. Данные бенчмарка (IMOEX)")
    print("-" * 60)

    if benchmark is None:
        print("Данные бенчмарка не загружены.")
        return

    print(f"Строк: {len(benchmark)}")
    print(f"Период: {benchmark['date'].min().strftime('%Y-%m-%d')} .. "
          f"{benchmark['date'].max().strftime('%Y-%m-%d')}")
    print(f"Колонки: {list(benchmark.columns)}")

    # проверяем, есть ли значение индекса
    value_cols = [c for c in benchmark.columns if c not in ("date", "ticker")]
    for col in value_cols[:3]:
        if benchmark[col].dtype in [np.float64, np.int64, float, int]:
            print(f"  {col}: min={benchmark[col].min():.2f}, "
                  f"max={benchmark[col].max():.2f}, "
                  f"NaN={benchmark[col].isna().sum()}")


def validate_risk_free(risk_free: pd.DataFrame):
    """Базовая проверка безрисковой ставки."""
    print("\n" + "-" * 60)
    print("7. Безрисковая ставка (ключевая ставка ЦБ)")
    print("-" * 60)

    if risk_free is None:
        print("Данные безрисковой ставки не загружены.")
        return

    print(f"Строк: {len(risk_free)}")
    print(f"Колонки: {list(risk_free.columns)}")

    if "date" in risk_free.columns:
        print(f"Период: {risk_free['date'].min()} .. {risk_free['date'].max()}")

    rate_cols = [c for c in risk_free.columns if c not in ("date",)]
    for col in rate_cols[:3]:
        if risk_free[col].dtype in [np.float64, np.int64, float, int]:
            print(f"  {col}: min={risk_free[col].min():.2f}%, "
                  f"max={risk_free[col].max():.2f}%")


def apply_split_adjustments(ohlcv: pd.DataFrame, corp_actions: pd.DataFrame) -> pd.DataFrame:
    """
    Backward-корректировка на сплиты.
    Для каждого сплита: все цены ДО даты сплита делим на ratio,
    объемы ДО даты сплита умножаем на ratio.
    """
    print("\n" + "-" * 60)
    print("8. Корректировка на сплиты (backward adjustment)")
    print("-" * 60)

    if corp_actions.empty:
        print("Сплитов не обнаружено, корректировка не требуется.")
        return ohlcv

    df = ohlcv.copy()
    price_cols = [c for c in ["open", "high", "low", "close", "close_official", "waprice"]
                  if c in df.columns]

    for _, action in corp_actions.iterrows():
        if action["action_type"] not in ("split", "reverse_split"):
            continue

        ticker = action["ticker"]
        split_date = pd.to_datetime(action["date"])
        ratio = action["ratio"]

        mask = (df["ticker"] == ticker) & (df["date"] < split_date)
        n_adjusted = mask.sum()

        if action["action_type"] == "split":
            # прямой сплит: цены до сплита нужно разделить на ratio
            for col in price_cols:
                df.loc[mask, col] = df.loc[mask, col] / ratio
            if "volume" in df.columns:
                df.loc[mask, "volume"] = df.loc[mask, "volume"] * ratio
            print(f"  {ticker}: сплит 1:{ratio} от {split_date.strftime('%Y-%m-%d')}, "
                  f"скорректировано {n_adjusted} строк")
        elif action["action_type"] == "reverse_split":
            # обратный сплит: цены до сплита умножаем на ratio
            for col in price_cols:
                df.loc[mask, col] = df.loc[mask, col] * ratio
            if "volume" in df.columns:
                df.loc[mask, "volume"] = df.loc[mask, "volume"] / ratio
            print(f"  {ticker}: обратный сплит {ratio}:1 от {split_date.strftime('%Y-%m-%d')}, "
                  f"скорректировано {n_adjusted} строк")

    return df


def create_final_dataset(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    Формируем финальный датасет prices_adjusted.parquet.
    Берем close_official (LEGALCLOSEPRICE) если есть, иначе close.
    Заполняем пропуски forward fill с лимитом 5 дней.
    """
    print("\n" + "-" * 60)
    print("9. Формирование финального датасета")
    print("-" * 60)

    # выбираем цену закрытия: приоритет у LEGALCLOSEPRICE (официальная цена закрытия)
    if "close_official" in ohlcv.columns:
        ohlcv["close_adj"] = ohlcv["close_official"].fillna(ohlcv["close"])
        n_official = ohlcv["close_official"].notna().sum()
        n_fallback = ohlcv["close_official"].isna().sum()
        print(f"Источник цены: close_official (LEGALCLOSEPRICE) — {n_official} значений")
        print(f"Fallback на close: {n_fallback} значений")
    else:
        ohlcv["close_adj"] = ohlcv["close"]
        print("Колонка close_official не найдена, используем close")

    # формируем итоговый long-format DataFrame
    result = ohlcv[["date", "ticker", "close_adj"]].copy()
    result = result.sort_values(["ticker", "date"]).reset_index(drop=True)

    # forward fill пропусков (limit=5 дней, чтобы не протягивать через длинные паузы)
    n_nan_before = result["close_adj"].isna().sum()
    if n_nan_before > 0:
        result["close_adj"] = result.groupby("ticker")["close_adj"].transform(
            lambda x: x.ffill(limit=5)
        )
        n_nan_after = result["close_adj"].isna().sum()
        print(f"Forward fill: {n_nan_before} NaN до, {n_nan_after} NaN после (limit=5)")
    else:
        print("NaN в close_adj: нет, forward fill не потребовался")

    # убираем строки, где close_adj все равно NaN (не удалось заполнить)
    remaining_nan = result["close_adj"].isna().sum()
    if remaining_nan > 0:
        nan_tickers = result[result["close_adj"].isna()]["ticker"].unique()
        print(f"Оставшиеся NaN: {remaining_nan} (тикеры: {', '.join(nan_tickers)})")
        # не удаляем — просто предупреждаем

    print(f"\nИтоговый датасет: {len(result)} строк, "
          f"{result['ticker'].nunique()} тикеров")
    print(f"Период: {result['date'].min().strftime('%Y-%m-%d')} .. "
          f"{result['date'].max().strftime('%Y-%m-%d')}")

    return result


def print_final_report(result: pd.DataFrame, ohlcv: pd.DataFrame,
                       benchmark, risk_free, corp_actions):
    """Итоговый отчет о валидации."""
    print("\n" + "=" * 60)
    print("ИТОГОВЫЙ ОТЧЕТ О ВАЛИДАЦИИ")
    print("=" * 60)

    print(f"\nОсновной датасет (OHLCV):")
    print(f"  Строк: {len(ohlcv)}")
    print(f"  Тикеров: {ohlcv['ticker'].nunique()}")
    print(f"  Период: {ohlcv['date'].min().strftime('%Y-%m-%d')} .. "
          f"{ohlcv['date'].max().strftime('%Y-%m-%d')}")
    print(f"  Уникальных дат: {ohlcv['date'].nunique()}")

    print(f"\nФинальный датасет (prices_adjusted):")
    print(f"  Строк: {len(result)}")
    print(f"  Тикеров: {result['ticker'].nunique()}")
    print(f"  NaN в close_adj: {result['close_adj'].isna().sum()}")

    if benchmark is not None:
        print(f"\nБенчмарк (IMOEX): {len(benchmark)} строк")
    else:
        print(f"\nБенчмарк: не загружен")

    if risk_free is not None:
        print(f"Безрисковая ставка: {len(risk_free)} записей")
    else:
        print(f"Безрисковая ставка: не загружена")

    print(f"Корпоративные действия: {len(corp_actions)} записей")

    print(f"\nФайлы:")
    print(f"  {OHLCV_PATH}")
    if BENCHMARK_PATH.exists():
        print(f"  {BENCHMARK_PATH}")
    if RISK_FREE_PATH.exists():
        print(f"  {RISK_FREE_PATH}")
    print(f"  {CORP_ACTIONS_PATH}")
    print(f"  {OUTPUT_PATH}")

    # дополнительная статистика по финальному датасету
    print(f"\nСтатистика close_adj по тикерам (финальный датасет):")
    stats = result.groupby("ticker")["close_adj"].agg(["count", "mean", "min", "max"])
    stats.columns = ["count", "mean", "min", "max"]
    stats = stats.sort_values("count", ascending=False)

    print(f"\n{'Тикер':<8} {'Дней':>6} {'Средняя':>10} {'Мин':>10} {'Макс':>10}")
    print("-" * 48)
    for ticker, row in stats.iterrows():
        print(f"{ticker:<8} {row['count']:>6.0f} {row['mean']:>10.2f} "
              f"{row['min']:>10.2f} {row['max']:>10.2f}")


def main():
    print("=" * 70)
    print("Скрипт 04: Валидация данных и формирование финального датасета")
    print("=" * 70)

    # загружаем все данные
    ohlcv, benchmark, risk_free, calendar, corp_actions = load_all_data()

    # последовательные проверки
    validate_row_counts(ohlcv)
    n_dupes = validate_duplicates(ohlcv)

    # если есть дубликаты — удаляем, оставляя последнюю запись
    if n_dupes > 0:
        ohlcv = ohlcv.drop_duplicates(subset=["date", "ticker"], keep="last")
        print(f"Дубликаты удалены, осталось {len(ohlcv)} строк")

    validate_prices(ohlcv)
    validate_calendar(ohlcv, calendar)
    report_halt_period(ohlcv)
    validate_benchmark(benchmark)
    validate_risk_free(risk_free)

    # корректировка на сплиты
    ohlcv = apply_split_adjustments(ohlcv, corp_actions)

    # формируем финальный датасет
    result = create_final_dataset(ohlcv)

    # сохраняем
    result.to_parquet(OUTPUT_PATH, index=False)
    print(f"\nФинальный датасет сохранен: {OUTPUT_PATH}")

    # итоговый отчет
    print_final_report(result, ohlcv, benchmark, risk_free, corp_actions)

    print("\nВалидация завершена.")


if __name__ == "__main__":
    main()
