"""
Базовый модуль оптимизации портфелей по Марковицу.
Используется всеми скриптами step3 для построения efficient frontier.
"""

import numpy as np
import pandas as pd
import pickle
from pathlib import Path
from scipy.optimize import minimize

np.random.seed(42)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
PROCESSED = DATA_DIR / "processed"
RAW = DATA_DIR / "raw"
TEMP = Path(__file__).resolve().parent.parent / "temp"


# --- загрузка данных ---

def load_selected_data():
    """Загрузить выбранные mu и Sigma из step3_select_window."""
    mu_df = pd.read_parquet(PROCESSED / "selected_mu.parquet")
    cov_df = pd.read_parquet(PROCESSED / "selected_cov.parquet")

    tickers = mu_df['ticker'].tolist()
    mu = mu_df['expected_return'].values
    cov = cov_df.values

    return mu, cov, tickers


def load_risk_free_rate():
    """Загрузить безрисковую ставку (годовую) на выбранную дату."""
    rf_df = pd.read_parquet(PROCESSED / "selected_rf.parquet")
    return rf_df['rf_annual'].iloc[0]


# --- целевая функция ---

def portfolio_variance(w, cov):
    """Дисперсия портфеля: w^T @ Sigma @ w."""
    return w @ cov @ w


def portfolio_volatility(w, cov):
    """Волатильность портфеля (годовая)."""
    return np.sqrt(w @ cov @ w)


def portfolio_return(w, mu):
    """Ожидаемая доходность портфеля."""
    return w @ mu


def portfolio_sharpe(w, mu, cov, rf):
    """Коэффициент Шарпа портфеля."""
    ret = w @ mu
    vol = np.sqrt(w @ cov @ w)
    if vol < 1e-10:
        return 0.0
    return (ret - rf) / vol


# --- оптимизация отдельного портфеля ---

def optimize_for_target(mu, cov, target_return, bounds=None):
    """
    Найти портфель с минимальной дисперсией для заданной целевой доходности.

    Задача:
      min  w^T Sigma w
      s.t. w^T mu = target_return
           sum(w) = 1
           + bounds на отдельные веса (если заданы)
    """
    n = len(mu)
    x0 = np.ones(n) / n

    constraints = [
        {'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0},
        {'type': 'eq', 'fun': lambda w: w @ mu - target_return},
    ]

    result = minimize(
        portfolio_variance, x0, args=(cov,),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'ftol': 1e-12, 'maxiter': 1000}
    )
    return result


# --- ключевые портфели ---

def find_gmvp(mu, cov, bounds=None):
    """
    Global Minimum Variance Portfolio — портфель с минимальным риском.
    Не зависит от вектора ожидаемых доходностей.

      min  w^T Sigma w
      s.t. sum(w) = 1
           + bounds
    """
    n = len(mu)
    x0 = np.ones(n) / n

    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}]

    result = minimize(
        portfolio_variance, x0, args=(cov,),
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'ftol': 1e-12, 'maxiter': 1000}
    )

    if not result.success:
        print(f"  [!] GMVP: optimizer failed -- {result.message}")

    w = result.x
    ret = w @ mu
    vol = np.sqrt(w @ cov @ w)

    return {
        'weights': w,
        'return': ret,
        'std': vol,
        'success': result.success,
    }


def find_tangency(mu, cov, rf, bounds=None):
    """
    Tangency portfolio -- максимальный Sharpe ratio.
    Используем SLSQP с несколькими стартовыми точками.
    Для unrestricted: дополнительная проверка через аналитику.
    """
    n = len(mu)
    excess = mu - rf
    best = None

    # стартовые точки: равные веса + концентрация на лучших по excess return
    top_idx = np.argsort(excess)[::-1]
    starts = [np.ones(n) / n]
    for k in [1, 3, 5, 10]:
        x0 = np.zeros(n)
        if bounds is not None:
            lb = np.array([b[0] if b[0] is not None else -10.0 for b in bounds])
            x0[:] = np.maximum(lb, 0.0)
            remaining = 1.0 - x0.sum()
            if remaining > 0:
                x0[top_idx[:k]] += remaining / k
            else:
                x0 = np.ones(n) / n
        else:
            x0[top_idx[:k]] = 1.0 / k
        starts.append(x0)

    def neg_sharpe(w):
        ret = w @ mu
        vol = np.sqrt(w @ cov @ w)
        if vol < 1e-10:
            return 0.0
        return -(ret - rf) / vol

    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}]

    for x0 in starts:
        result = minimize(
            neg_sharpe, x0,
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            options={'ftol': 1e-12, 'maxiter': 2000}
        )
        if result.success:
            w = result.x
            ret = w @ mu
            vol = np.sqrt(w @ cov @ w)
            sharpe = (ret - rf) / vol if vol > 1e-10 else 0.0
            if best is None or sharpe > best['sharpe']:
                best = {
                    'weights': w.copy(),
                    'return': ret,
                    'std': vol,
                    'sharpe': sharpe,
                    'success': True,
                }

    # для unrestricted: проверяем аналитическое решение
    if bounds is None:
        Sigma_inv = np.linalg.inv(cov)
        w_raw = Sigma_inv @ excess
        denom = np.sum(w_raw)
        if abs(denom) > 1e-10:
            w_an = w_raw / denom
            ret_an = w_an @ mu
            vol_an = np.sqrt(w_an @ cov @ w_an)
            sharpe_an = (ret_an - rf) / vol_an if vol_an > 1e-10 else 0.0
            if best is None or sharpe_an > best['sharpe']:
                best = {
                    'weights': w_an.copy(),
                    'return': ret_an,
                    'std': vol_an,
                    'sharpe': sharpe_an,
                    'success': True,
                }

    if best is None:
        print("  [!] Tangency: ни один старт не дал результат")
        gmvp = find_gmvp(mu, cov, bounds)
        gmvp['sharpe'] = (gmvp['return'] - rf) / gmvp['std']
        return gmvp

    return best


def find_max_return(mu, cov, bounds=None):
    """
    Портфель с максимальной доходностью при заданных ограничениях.
    Правая крайняя точка на границе.
    """
    n = len(mu)
    x0 = np.ones(n) / n

    def neg_return(w):
        return -(w @ mu)

    constraints = [{'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0}]

    result = minimize(
        neg_return, x0,
        method='SLSQP',
        bounds=bounds,
        constraints=constraints,
        options={'ftol': 1e-12, 'maxiter': 1000}
    )

    w = result.x
    ret = w @ mu
    vol = np.sqrt(w @ cov @ w)

    return {
        'weights': w,
        'return': ret,
        'std': vol,
        'success': result.success,
    }


# --- граница эффективных портфелей ---

def build_efficient_frontier(mu, cov, rf, n_points=200, bounds=None,
                             mu_min=None, mu_max=None):
    """
    Построить границу эффективных портфелей.
    Перебираем целевые доходности от GMVP до max_return.

    Tangency определяется как точка с максимальным Sharpe на построенной границе
    (более робастно, чем отдельная оптимизация для случаев с rf > GMVP return).

    Возвращает:
      frontier_df, frontier_weights, key_portfolios
    """
    n = len(mu)

    # GMVP
    gmvp = find_gmvp(mu, cov, bounds)
    if mu_min is None:
        mu_min = gmvp['return']

    # максимальная доходность
    if mu_max is None:
        max_ret = find_max_return(mu, cov, bounds)
        mu_max = max_ret['return']

    print(f"  GMVP:     return={gmvp['return']*100:.2f}%, std={gmvp['std']*100:.2f}%")
    print(f"  Диапазон: {mu_min*100:.2f}% -- {mu_max*100:.2f}%")

    # строим границу
    targets = np.linspace(mu_min, mu_max, n_points)

    records = []
    weights_list = []
    failed = 0

    for target in targets:
        res = optimize_for_target(mu, cov, target, bounds)
        if res.success:
            w = res.x
            ret = w @ mu
            vol = np.sqrt(w @ cov @ w)
            sharpe = (ret - rf) / vol if vol > 1e-10 else 0.0
            records.append({
                'target_return': target,
                'portfolio_return': ret,
                'portfolio_std': vol,
                'sharpe': sharpe,
            })
            weights_list.append(w.copy())
        else:
            failed += 1

    if failed > 0:
        print(f"  Не сошлось: {failed} из {n_points}")

    frontier_df = pd.DataFrame(records)
    frontier_weights = np.array(weights_list) if weights_list else np.empty((0, n))

    # tangency: точка с максимальным Sharpe на построенной границе
    if len(frontier_df) > 0:
        best_idx = frontier_df['sharpe'].idxmax()
        tangency = {
            'weights': frontier_weights[best_idx].copy(),
            'return': frontier_df.iloc[best_idx]['portfolio_return'],
            'std': frontier_df.iloc[best_idx]['portfolio_std'],
            'sharpe': frontier_df.iloc[best_idx]['sharpe'],
            'success': True,
        }
    else:
        tangency = gmvp.copy()
        tangency['sharpe'] = (tangency['return'] - rf) / tangency['std']

    print(f"  Tangency: return={tangency['return']*100:.2f}%, "
          f"std={tangency['std']*100:.2f}%, sharpe={tangency['sharpe']:.4f}")
    print(f"  Точек на границе: {len(frontier_df)}")

    key_portfolios = {
        'gmvp': gmvp,
        'tangency': tangency,
    }

    return frontier_df, frontier_weights, key_portfolios


# --- аналитическое решение (для верификации unrestricted) ---

def analytical_frontier(mu, cov, rf, n_points=200, mu_min=None, mu_max=None):
    """
    Closed-form решение задачи Марковица без ограничений.
    Формулы из Merton (1972). Используется для cross-check.
    """
    n = len(mu)
    ones = np.ones(n)
    Sigma_inv = np.linalg.inv(cov)

    A = ones @ Sigma_inv @ mu
    B = mu @ Sigma_inv @ mu
    C = ones @ Sigma_inv @ ones
    D = B * C - A * A

    # GMVP
    w_gmvp = Sigma_inv @ ones / C
    mu_gmvp = w_gmvp @ mu
    sig_gmvp = np.sqrt(w_gmvp @ cov @ w_gmvp)

    if mu_min is None:
        mu_min = mu_gmvp
    if mu_max is None:
        mu_max = mu_min + 2 * (np.max(mu) - mu_min)

    targets = np.linspace(mu_min, mu_max, n_points)
    records = []
    weights_list = []

    for target in targets:
        lam1 = (C * target - A) / D
        lam2 = (B - A * target) / D
        w = Sigma_inv @ (lam1 * mu + lam2 * ones)

        ret = w @ mu
        vol = np.sqrt(w @ cov @ w)
        sharpe = (ret - rf) / vol if vol > 1e-10 else 0.0

        records.append({
            'target_return': target,
            'portfolio_return': ret,
            'portfolio_std': vol,
            'sharpe': sharpe,
        })
        weights_list.append(w.copy())

    frontier_df = pd.DataFrame(records)
    frontier_weights = np.array(weights_list)

    # tangency: max Sharpe на аналитической границе
    best_idx = frontier_df['sharpe'].idxmax()

    key_portfolios = {
        'gmvp': {
            'weights': w_gmvp,
            'return': mu_gmvp,
            'std': sig_gmvp,
            'success': True,
        },
        'tangency': {
            'weights': frontier_weights[best_idx].copy(),
            'return': frontier_df.iloc[best_idx]['portfolio_return'],
            'std': frontier_df.iloc[best_idx]['portfolio_std'],
            'sharpe': frontier_df.iloc[best_idx]['sharpe'],
            'success': True,
        },
    }

    return frontier_df, frontier_weights, key_portfolios


# --- вспомогательные функции ---

def print_portfolio_summary(name, portfolio, tickers):
    """Вывести сводку по портфелю: топ акций, экстремальные веса."""
    w = portfolio['weights']
    sorted_idx = np.argsort(w)[::-1]

    print(f"\n  {name}:")
    print(f"    Return: {portfolio['return']*100:.2f}%")
    print(f"    Std:    {portfolio['std']*100:.2f}%")
    if 'sharpe' in portfolio:
        print(f"    Sharpe: {portfolio['sharpe']:.4f}")
    print(f"    Max weight: {w.max()*100:.2f}% ({tickers[np.argmax(w)]})")
    print(f"    Min weight: {w.min()*100:.2f}% ({tickers[np.argmin(w)]})")
    print(f"    N(w > 5%%): {np.sum(w > 0.05)}")
    print(f"    N(w < 0):   {np.sum(w < -0.001)}")

    print(f"    Top-5:")
    for rank, idx in enumerate(sorted_idx[:5]):
        print(f"      {rank+1}. {tickers[idx]:>8s}: {w[idx]*100:7.2f}%")


def save_frontier(frontier_df, name, output_dir=None):
    """Сохранить границу в parquet."""
    if output_dir is None:
        output_dir = PROCESSED
    path = output_dir / f"ef_{name}.parquet"
    frontier_df.to_parquet(path, index=False)
    print(f"  Saved: {path.name} ({len(frontier_df)} points)")
    return path


if __name__ == '__main__':
    print("Checking step3_optimizer module...")

    mu, cov, tickers = load_selected_data()
    rf = load_risk_free_rate()

    print(f"Tickers: {len(tickers)}")
    print(f"rf = {rf*100:.1f}%")
    print(f"mu: [{mu.min()*100:.1f}%, {mu.max()*100:.1f}%]")

    # GMVP без ограничений
    gmvp = find_gmvp(mu, cov)
    print(f"\nGMVP (unrestricted):")
    print(f"  return={gmvp['return']*100:.2f}%, std={gmvp['std']*100:.2f}%")
    print(f"  sum(w)={gmvp['weights'].sum():.6f}")

    # tangency без ограничений
    tang = find_tangency(mu, cov, rf)
    print(f"\nTangency (unrestricted):")
    print(f"  return={tang['return']*100:.2f}%, std={tang['std']*100:.2f}%, sharpe={tang['sharpe']:.4f}")

    # tangency с bounds (long only) -- для проверки
    bounds_lo = [(0, 1)] * len(mu)
    tang_lo = find_tangency(mu, cov, rf, bounds=bounds_lo)
    print(f"\nTangency (long only):")
    print(f"  return={tang_lo['return']*100:.2f}%, std={tang_lo['std']*100:.2f}%, sharpe={tang_lo['sharpe']:.4f}")

    # мини-граница (20 точек) для быстрой проверки
    print("\nBuilding test frontier (long only, 20 pts)...")
    ef_df, ef_w, kp = build_efficient_frontier(mu, cov, rf, n_points=20,
                                                bounds=bounds_lo)
    print(f"  Frontier tangency sharpe: {kp['tangency']['sharpe']:.4f}")

    print("\nModule OK.")
