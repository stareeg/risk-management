"""
Общий модуль для построения серий efficient frontiers на разных датах.
Используется скриптами step4_rolling, step4_expanding, step4_ewma.
Импортирует функции оптимизации из step3_optimizer.
"""

import sys
import numpy as np
import pandas as pd
import pickle
import warnings
from pathlib import Path
from datetime import timedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.colors import Normalize

sys.path.insert(0, str(Path(__file__).resolve().parent))
from step3_optimizer import (
    build_efficient_frontier, find_gmvp,
    PROCESSED, RAW, TEMP,
)

np.random.seed(42)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# --- загрузка серий оценок (mu, Sigma) ---

def load_estimation_series(method):
    """
    Загружает серию (dates, means, covs, tickers) для указанного метода оценки.

    method: 'rolling_252d', 'rolling_63d', 'rolling_21d',
            'expanding', 'ewma_094', 'ewma_097', 'ewma_099'

    Returns:
      dates:   list of datetime.date
      means:   list of np.array(30,)
      covs:    list of np.array(30, 30)
      tickers: list of str
    """
    if method.startswith('rolling_'):
        window = method.replace('rolling_', '')  # '252d', '63d', '21d'
        means_path = PROCESSED / f"rolling_{window}_means.parquet"
        covs_path = PROCESSED / f"rolling_{window}_covs.pkl"

    elif method == 'expanding':
        means_path = PROCESSED / "expanding_means.parquet"
        covs_path = PROCESSED / "expanding_covs.pkl"

    elif method.startswith('ewma_'):
        # ewma_094, ewma_097, ewma_099
        lam = method.replace('ewma_', '')  # '094', '097', '099'
        means_path = PROCESSED / "ewma_means.parquet"
        covs_path = PROCESSED / f"ewma_{lam}_covs.pkl"

    else:
        raise ValueError(f"Неизвестный метод: {method}")

    # средние доходности — parquet с DatetimeIndex и 30 колонками-тикерами
    means_df = pd.read_parquet(means_path)
    tickers = means_df.columns.tolist()

    # ковариационные матрицы — pickle: dict с ключами dates, tickers, covs
    with open(covs_path, 'rb') as f:
        covs_data = pickle.load(f)

    covs_dates = covs_data['dates']
    covs_array = covs_data['covs']  # shape (n_dates, 30, 30)

    # проверяем согласованность
    assert len(means_df) == len(covs_dates), (
        f"Количество дат не совпадает: means={len(means_df)}, covs={len(covs_dates)}"
    )

    # собираем в списки
    dates = [d.date() if hasattr(d, 'date') else d for d in means_df.index]
    means = [means_df.iloc[i].values for i in range(len(means_df))]
    covs = [covs_array[i] for i in range(len(covs_array))]

    return dates, means, covs, tickers


# --- безрисковая ставка ---

def load_historical_rf():
    """
    Загружает ключевую ставку ЦБ из risk_free_rate.parquet.
    Returns: pd.Series с DatetimeIndex, значения — годовая ставка (доля, не %).
    """
    rf_df = pd.read_parquet(RAW / "risk_free_rate.parquet")
    # колонки: date, rate_annual, rate_daily, rate_daily_log
    rf_series = rf_df.set_index('date')['rate_annual']
    rf_series.index = pd.to_datetime(rf_series.index)
    rf_series = rf_series.sort_index()
    return rf_series


def get_rf_for_date(rf_series, date):
    """
    Безрисковая ставка на заданную дату (forward fill — последнее известное значение).
    """
    # приводим date к Timestamp для сравнения с индексом
    if not isinstance(date, pd.Timestamp):
        date = pd.Timestamp(date)

    # берем все значения до date включительно
    mask = rf_series.index <= date
    if mask.any():
        return rf_series[mask].iloc[-1]

    # если дата раньше всех данных — первое известное
    return rf_series.iloc[0]


# --- подвыборка дат ---

def subsample_dates(dates, step='annual'):
    """
    Выбирает индексы дат с заданным шагом.

    step: 'annual' (~252 дней), 'quarterly' (~63 дня),
          'monthly' (~21 день), 'all' (все даты),
          'year_end' (ближайшие к концам календарных лет)

    Returns: list of int — индексы в исходном массиве dates
    """
    if step == 'all':
        return list(range(len(dates)))

    if step == 'annual' or step == 'year_end':
        # привязка к концам календарных лет — интуитивнее для интерпретации
        return _subsample_year_end(dates)

    step_days = {
        'quarterly': 63,
        'monthly': 21,
    }

    if step not in step_days:
        raise ValueError(f"Неизвестный шаг: {step}")

    gap = step_days[step]
    indices = [0]
    last_date = dates[0]

    for i in range(1, len(dates)):
        delta = (dates[i] - last_date).days
        if delta >= gap:
            indices.append(i)
            last_date = dates[i]

    # всегда включаем последнюю дату
    if indices[-1] != len(dates) - 1:
        indices.append(len(dates) - 1)

    return indices


def _subsample_year_end(dates):
    """
    Выбирает даты, ближайшие к концу каждого календарного года.
    Более интуитивно для интерпретации (конец 2016, конец 2017, ...).
    """
    from collections import defaultdict

    # группируем по году
    year_indices = defaultdict(list)
    for i, d in enumerate(dates):
        year_indices[d.year].append(i)

    indices = []
    for year in sorted(year_indices.keys()):
        # берем последнюю дату в каждом году
        indices.append(year_indices[year][-1])

    return indices


# --- построение серии фронтиров ---

def build_frontier_series(method, step='annual', n_points=100, bounds=None):
    """
    Строит efficient frontier для каждой даты из подвыборки.

    Params:
      method:   метод оценки ('rolling_252d', 'expanding', 'ewma_094' и т.д.)
      step:     шаг подвыборки ('annual', 'quarterly', 'year_end', 'all')
      n_points: точек на фронтире (100 по умолчанию)
      bounds:   ограничения (None = unrestricted)

    Returns: dict с ключами:
      'method', 'step', 'dates', 'frontiers', 'key_portfolios',
      'rf_values', 'tickers', 'frontier_weights'
    """
    print(f"\n{'='*60}")
    print(f"Построение серии фронтиров: {method}, шаг={step}")
    print(f"{'='*60}")

    # загружаем серию оценок
    dates, means, covs, tickers = load_estimation_series(method)
    print(f"Загружено {len(dates)} дат, {len(tickers)} тикеров")

    # подвыбираем даты
    indices = subsample_dates(dates, step=step)
    print(f"Подвыборка ({step}): {len(indices)} дат")

    # историческая безрисковая ставка
    rf_series = load_historical_rf()

    result = {
        'method': method,
        'step': step,
        'dates': [],
        'frontiers': [],
        'frontier_weights': [],
        'key_portfolios': [],
        'rf_values': [],
        'tickers': tickers,
    }

    for k, idx in enumerate(indices):
        date_i = dates[idx]
        mu_i = means[idx]
        sigma_i = covs[idx]
        rf_i = get_rf_for_date(rf_series, date_i)

        print(f"\n  [{k+1}/{len(indices)}] {date_i}, rf={rf_i*100:.1f}%")

        # GMVP для определения mu_max
        gmvp = find_gmvp(mu_i, sigma_i, bounds)

        # mu_max по формуле из step_3 с fallback
        max_mu = np.max(mu_i)
        if max_mu > gmvp['return']:
            mu_max_val = gmvp['return'] + 2.0 * (max_mu - gmvp['return'])
        else:
            # fallback: все акции хуже GMVP (маловероятно, но возможно)
            mu_max_val = max_mu + abs(max_mu) * 0.5
            if mu_max_val <= gmvp['return']:
                mu_max_val = gmvp['return'] + 0.05  # минимальный запас 5%

        try:
            frontier_df, weights, key_ports = build_efficient_frontier(
                mu_i, sigma_i, rf_i,
                n_points=n_points,
                bounds=bounds,
                mu_max=mu_max_val,
            )

            result['dates'].append(date_i)
            result['frontiers'].append(frontier_df)
            result['frontier_weights'].append(weights)
            result['key_portfolios'].append(key_ports)
            result['rf_values'].append(rf_i)

            gmvp_info = key_ports['gmvp']
            print(f"    GMVP: ret={gmvp_info['return']*100:.2f}%, "
                  f"std={gmvp_info['std']*100:.2f}%, "
                  f"точек={len(frontier_df)}")

        except Exception as e:
            warnings.warn(f"Не удалось построить фронтир для {date_i}: {e}")
            print(f"    ОШИБКА: {e}")
            continue

    print(f"\nПостроено {len(result['dates'])} фронтиров из {len(indices)} запрошенных")
    return result


# --- сохранение / загрузка ---

def save_dynamics(result, name):
    """Сохраняет результат в data/processed/ef_dynamics_{name}.pkl"""
    out_path = PROCESSED / f"ef_dynamics_{name}.pkl"
    with open(out_path, 'wb') as f:
        pickle.dump(result, f)
    print(f"Сохранено: {out_path}")
    return out_path


def load_dynamics(name):
    """Загружает из data/processed/ef_dynamics_{name}.pkl"""
    in_path = PROCESSED / f"ef_dynamics_{name}.pkl"
    with open(in_path, 'rb') as f:
        return pickle.load(f)


# --- визуализация ---

def plot_frontier_dynamics(result, title, output_path, highlight_dates=None):
    """
    Overlay нескольких фронтиров на одном графике (sigma vs return).
    Каждый фронтир — линия с цветом от холодного (ранние) к теплому (поздние).
    GMVP каждого фронтира — маркер.
    """
    dates = result['dates']
    frontiers = result['frontiers']
    key_ports = result['key_portfolios']
    n = len(dates)

    fig, ax = plt.subplots(figsize=(12, 8))

    # цветовая палитра: от синего к красному
    norm = Normalize(vmin=0, vmax=max(n - 1, 1))
    cmap = cm.coolwarm

    for i in range(n):
        df = frontiers[i]
        if df.empty:
            continue

        color = cmap(norm(i))
        year_label = str(dates[i].year) if hasattr(dates[i], 'year') else str(dates[i])

        # линия фронтира
        ax.plot(df['portfolio_std'] * 100, df['portfolio_return'] * 100,
                color=color, linewidth=1.5, alpha=0.8, label=year_label)

        # GMVP — маркер
        gmvp = key_ports[i]['gmvp']
        ax.scatter(gmvp['std'] * 100, gmvp['return'] * 100,
                   color=color, s=50, zorder=5, edgecolors='black', linewidths=0.5)

    ax.set_xlabel('Годовая волатильность, %', fontsize=12)
    ax.set_ylabel('Годовая доходность, %', fontsize=12)
    ax.set_title(title, fontsize=14)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"График: {output_path}")


def plot_gmvp_trajectory(results_dict, title, output_path):
    """
    Траектория GMVP (return и std) во времени для разных методов.
    results_dict: {'rolling_252d': result, 'expanding': result, 'ewma_097': result}

    Два subplot: верхний — return GMVP, нижний — std GMVP, оба vs time.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

    colors = {
        'rolling_252d': '#2196F3',
        'expanding': '#4CAF50',
        'ewma_094': '#FF9800',
        'ewma_097': '#F44336',
        'ewma_099': '#9C27B0',
        'rolling_63d': '#00BCD4',
        'rolling_21d': '#795548',
    }

    labels = {
        'rolling_252d': 'Скольз. 252d',
        'expanding': 'Расширяющееся',
        'ewma_094': 'EWMA λ=0.94',
        'ewma_097': 'EWMA λ=0.97',
        'ewma_099': 'EWMA λ=0.99',
        'rolling_63d': 'Скольз. 63d',
        'rolling_21d': 'Скольз. 21d',
    }

    for method_name, result in results_dict.items():
        dates = result['dates']
        gmvp_returns = [kp['gmvp']['return'] * 100 for kp in result['key_portfolios']]
        gmvp_stds = [kp['gmvp']['std'] * 100 for kp in result['key_portfolios']]

        color = colors.get(method_name, '#666666')
        label = labels.get(method_name, method_name)

        ax1.plot(dates, gmvp_returns, 'o-', color=color, label=label, markersize=5)
        ax2.plot(dates, gmvp_stds, 'o-', color=color, label=label, markersize=5)

    ax1.set_ylabel('Доходность GMVP, %', fontsize=11)
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)

    ax2.set_ylabel('Волатильность GMVP, %', fontsize=11)
    ax2.set_xlabel('Дата', fontsize=11)
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)

    fig.suptitle(title, fontsize=14)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"График: {output_path}")


def plot_frontier_area(result, title, output_path):
    """
    Область покрытия фронтирами: заливка между min и max sigma
    для каждого уровня return. Показывает разброс фронтира во времени.
    """
    frontiers = result['frontiers']
    if not frontiers:
        return

    # общая сетка доходностей (пересечение всех фронтиров)
    all_returns = []
    for df in frontiers:
        if not df.empty:
            all_returns.extend(df['portfolio_return'].values)

    ret_min = max(df['portfolio_return'].min() for df in frontiers if not df.empty)
    ret_max = min(df['portfolio_return'].max() for df in frontiers if not df.empty)

    if ret_min >= ret_max:
        print(f"  Пропускаю area plot: нет общего диапазона доходностей")
        return

    ret_grid = np.linspace(ret_min, ret_max, 200)

    # для каждого уровня доходности находим min и max sigma по всем фронтирам
    std_min = np.full_like(ret_grid, np.inf)
    std_max = np.full_like(ret_grid, -np.inf)

    for df in frontiers:
        if df.empty:
            continue
        # интерполируем sigma по return для этого фронтира
        sorted_df = df.sort_values('portfolio_return')
        interp_std = np.interp(
            ret_grid,
            sorted_df['portfolio_return'].values,
            sorted_df['portfolio_std'].values,
        )
        std_min = np.minimum(std_min, interp_std)
        std_max = np.maximum(std_max, interp_std)

    fig, ax = plt.subplots(figsize=(10, 7))

    ax.fill_betweenx(ret_grid * 100, std_min * 100, std_max * 100,
                      alpha=0.3, color='steelblue', label='Область покрытия')

    # первый и последний фронтир поверх
    for i, label_suffix in [(0, 'первый'), (-1, 'последний')]:
        df = frontiers[i]
        if not df.empty:
            date_label = result['dates'][i]
            ax.plot(df['portfolio_std'] * 100, df['portfolio_return'] * 100,
                    linewidth=2 if i == -1 else 1.2,
                    linestyle='-' if i == -1 else '--',
                    label=f'{date_label} ({label_suffix})')

    ax.set_xlabel('Годовая волатильность, %', fontsize=12)
    ax.set_ylabel('Годовая доходность, %', fontsize=12)
    ax.set_title(title, fontsize=14)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"График: {output_path}")


# --- вспомогательные ---

def print_series_summary(result):
    """Выводит сводку по серии фронтиров."""
    print(f"\nМетод: {result['method']}, шаг: {result['step']}")
    print(f"{'Дата':>12} | {'rf,%':>6} | {'GMVP ret,%':>10} | {'GMVP std,%':>10} | "
          f"{'Max Sharpe':>10} | {'Точек':>6}")
    print("-" * 72)

    for i, date_i in enumerate(result['dates']):
        rf_i = result['rf_values'][i]
        kp = result['key_portfolios'][i]
        gmvp = kp['gmvp']
        tang = kp['tangency']
        n_pts = len(result['frontiers'][i])

        print(f"{str(date_i):>12} | {rf_i*100:>5.1f}% | {gmvp['return']*100:>9.2f}% | "
              f"{gmvp['std']*100:>9.2f}% | {tang['sharpe']:>10.4f} | {n_pts:>6}")


if __name__ == '__main__':
    # быстрая проверка загрузки
    print("Проверка загрузки данных...")

    for method in ['rolling_252d', 'expanding', 'ewma_094']:
        dates, means, covs, tickers = load_estimation_series(method)
        print(f"  {method}: {len(dates)} дат, {len(tickers)} тикеров, "
              f"mu shape={means[0].shape}, cov shape={covs[0].shape}")

    rf = load_historical_rf()
    print(f"  rf: {len(rf)} записей, от {rf.index.min().date()} до {rf.index.max().date()}")
    print(f"  rf на конец 2025: {get_rf_for_date(rf, pd.Timestamp('2025-12-30'))*100:.1f}%")

    # проверка подвыборки дат
    dates_252d, _, _, _ = load_estimation_series('rolling_252d')
    idx_annual = subsample_dates(dates_252d, step='annual')
    print(f"\n  rolling_252d annual: {len(idx_annual)} дат")
    for i in idx_annual:
        print(f"    {dates_252d[i]}")

    idx_ye = subsample_dates(dates_252d, step='year_end')
    print(f"\n  rolling_252d year_end: {len(idx_ye)} дат")
    for i in idx_ye:
        print(f"    {dates_252d[i]}")
