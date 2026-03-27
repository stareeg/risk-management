"""
Фаза A шага 2: расчёт дневных доходностей (простых и логарифмических).
Читаем prices_adjusted.parquet, считаем доходности, сохраняем в wide format.
"""
import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path("C:/Projects/risk_management/data")

# загружаем скорректированные цены (long format: date, ticker, close_adj)
prices = pd.read_parquet(DATA_DIR / "processed" / "prices_adjusted.parquet")
print(f"Загружено {len(prices)} строк, тикеров: {prices.ticker.nunique()}")

# разворачиваем в wide format: дата по строкам, тикеры по столбцам
prices_wide = prices.pivot(index="date", columns="ticker", values="close_adj")
prices_wide.sort_index(inplace=True)
print(f"Wide format: {prices_wide.shape}")
print(f"Диапазон дат: {prices_wide.index.min()} — {prices_wide.index.max()}")

# обрезаем до пересечения дат — убираем строки, где есть NaN хотя бы у одного тикера
# самый поздний старт у CBOM (~2015-07-01), поэтому первые полгода отпадут
prices_common = prices_wide.dropna()
print(f"После обрезки до пересечения: {prices_common.shape}")
print(f"Общий диапазон: {prices_common.index.min()} — {prices_common.index.max()}")

# простые дневные доходности: r_t = (P_t - P_{t-1}) / P_{t-1}
returns_simple = prices_common.pct_change().iloc[1:]  # первая строка — NaN после pct_change
print(f"Простые доходности: {returns_simple.shape}")

# лог-доходности для отчёта: r_t = ln(P_t / P_{t-1})
returns_log = np.log(prices_common / prices_common.shift(1)).iloc[1:]
print(f"Лог-доходности: {returns_log.shape}")

# проверяем, что нет NaN
assert returns_simple.notna().all().all(), "Есть NaN в простых доходностях"
assert returns_log.notna().all().all(), "Есть NaN в лог-доходностях"

# базовые проверки
n_rows, n_cols = returns_simple.shape
print(f"\nПроверки:")
print(f"  Размер: {n_rows} строк x {n_cols} тикеров")
assert n_cols == 30, f"Ожидалось 30 тикеров, получили {n_cols}"
assert n_rows >= 2500, f"Слишком мало строк: {n_rows}"

# средняя дневная доходность SBER
sber_mean = returns_simple["SBER"].mean()
print(f"  Средняя дневная доходность SBER: {sber_mean:.4%}")

# максимальная абсолютная доходность (ожидаем ~46% для SBER 24.02.2022)
max_abs = returns_simple.abs().max().max()
max_ticker = returns_simple.abs().max().idxmax()
print(f"  Макс. |доходность|: {max_abs:.2%} ({max_ticker})")

# сохраняем в parquet (wide format, индекс — дата)
output_path = DATA_DIR / "processed" / "returns_daily.parquet"
returns_simple.to_parquet(output_path)
print(f"\nСохранено: {output_path}")
print(f"  Размер файла: {output_path.stat().st_size / 1024:.1f} KB")

# лог-доходности тоже сохраняем — пригодятся для анализа
output_log = DATA_DIR / "processed" / "returns_daily_log.parquet"
returns_log.to_parquet(output_log)
print(f"Сохранено: {output_log}")

# сохраняем prices_common (wide format) — нужны для дальнейших расчётов
output_prices = DATA_DIR / "processed" / "prices_common_wide.parquet"
prices_common.to_parquet(output_prices)
print(f"Сохранено: {output_prices}")

# дополнительная статистика в temp/
desc = returns_simple.describe().T
desc["skew"] = returns_simple.skew()
desc["kurtosis"] = returns_simple.kurtosis()
desc["annualized_mean"] = returns_simple.mean() * 252
desc["annualized_vol"] = returns_simple.std() * np.sqrt(252)

temp_dir = Path("C:/Projects/risk_management/temp")
temp_dir.mkdir(exist_ok=True)
desc.to_csv(temp_dir / "returns_summary_stats.csv")
print(f"\nОписательная статистика сохранена: {temp_dir / 'returns_summary_stats.csv'}")
print(desc[["annualized_mean", "annualized_vol"]].to_string())
