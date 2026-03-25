# Управление портфелем: эффективные портфели на российских акциях

Студенческий проект по курсу "Управление портфелем" (магистратура). Применяем теорию Марковица на реальных данных 30 российских акций с Московской биржи за 2015--2025 годы: строим границы эффективных портфелей при различных ограничениях, считаем бета-коэффициенты по рыночной модели, сравниваем три подхода к оценке ковариационной матрицы (историческая, на основе historical beta, на основе adjusted beta).

Задание содержит 25 задач. Выполнены задачи 1--15 (включая бонусные). Остаются задачи 16--25, инструкции к ним подробно описаны ниже.

---

## Быстрый старт

### Локально

```bash
pip install -r requirements.txt
```

### В Google Colab

```python
!pip install -r requirements.txt
```

Дальше открываем `portfolio_analysis.ipynb` (или запускаем скрипты из `scripts/` по порядку) и выполняем ячейки. Данные скачиваются с MOEX ISS API и сайта ЦБ РФ -- ключи доступа не нужны.

Если хотите работать с уже скачанными данными (без повторной загрузки), все parquet-файлы лежат в `data/`. Для Excel-пользователей: сводный файл `data/export/data_export.xlsx` содержит все основные данные на отдельных листах.

---

## 30 акций в портфеле

Акции отобраны из индексов MOEXBC (голубые фишки) и IMOEX. 6 тикеров с неполной историей заменены на альтернативы с полным покрытием 2015--2025 (подробное обоснование -- в `tickers_30.md`).

| # | Тикер | Компания | Сектор | Примечание |
|---|-------|----------|--------|------------|
| 1 | SBER | Сбербанк | Финансы | |
| 2 | GAZP | Газпром | Нефть и газ | |
| 3 | LKOH | ЛУКОЙЛ | Нефть и газ | |
| 4 | GMKN | Норильский никель | Горная добыча | Сплит 1:100 (2020) |
| 5 | NVTK | НОВАТЭК | Нефть и газ | |
| 6 | ROSN | Роснефть | Нефть и газ | |
| 7 | PLZL | Полюс | Золотодобыча | Сплит 1:10 (2020) |
| 8 | AFLT | Аэрофлот | Транспорт | Замена YNDX |
| 9 | CBOM | МКБ | Финансы | Замена TCSG |
| 10 | MGNT | Магнит | Ритейл | |
| 11 | SNGS | Сургутнефтегаз | Нефть и газ | |
| 12 | CHMF | Северсталь | Металлургия | |
| 13 | ALRS | АЛРОСА | Горная добыча | |
| 14 | MOEX | Московская биржа | Финансы | |
| 15 | MTSS | МТС | Телеком | |
| 16 | VTBR | ВТБ | Финансы | Консолидация 5000:1 (2022) |
| 17 | NLMK | НЛМК | Металлургия | |
| 18 | PHOR | ФосАгро | Химия | |
| 19 | TATN | Татнефть | Нефть и газ | |
| 20 | PIKK | ПИК | Строительство | |
| 21 | HYDR | РусГидро | Электроэнергетика | Замена POLY |
| 22 | IRAO | Интер РАО | Электроэнергетика | |
| 23 | RUAL | РУСАЛ | Металлургия | |
| 24 | MAGN | ММК | Металлургия | |
| 25 | AFKS | АФК Система | Холдинг | |
| 26 | SNGSP | Сургутнефтегаз-п | Нефть и газ | Замена FIVE, привилегированные |
| 27 | RTKM | Ростелеком | Телеком | |
| 28 | FEES | ФСК-Россети | Электроэнергетика | |
| 29 | MSNG | Мосэнерго | Электроэнергетика | Замена FLOT |
| 30 | MVID | М.Видео | Ритейл | Замена OZON |

Замены тикеров: YNDX -> AFLT, POLY -> HYDR, TCSG -> CBOM, FIVE -> SNGSP, FLOT -> MSNG, OZON -> MVID. Причина замен -- отсутствие данных на MOEX за весь период 2015--2025 (IPO позже, делистинг, редомициляция).

---

## Статус задач (1--25)

Полный текст задания -- в `task.txt`. Задачи со звёздочкой (*) -- бонусные.

| # | Задача | Статус | Где результат |
|---|--------|--------|---------------|
| 1 | Сбор данных (30 акций, OHLCV, 2015--2025) | Выполнено | `data/raw/`, `data/processed/prices_adjusted.parquet` |
| 2a | Ковариационные матрицы, скользящее окно (252d, 63d, 21d) | Выполнено | `data/processed/rolling_*_covs.pkl` |
| 2b* | Ковариационные матрицы, расширяющееся окно | Выполнено | `data/processed/expanding_covs.pkl` |
| 3* | EWMA ковариация (lambda=0.94, 0.97, 0.99) | Выполнено | `data/processed/ewma_*_covs.pkl` |
| 4 | Выбор окна (rolling 252d, конец 2025-12-30) | Выполнено | `data/processed/selected_*.parquet` |
| 5 | EF без ограничений (unrestricted) | Выполнено | `data/processed/ef_unrestricted.parquet` |
| 6 | EF short <= 25% | Выполнено | `data/processed/ef_short_25.parquet` |
| 7 | EF long only | Выполнено | `data/processed/ef_long_only.parquet` |
| 8 | EF min 2% в каждую акцию | Выполнено | `data/processed/ef_min_2pct.parquet` |
| 9a | Динамика EF, скользящее окно 252d | Выполнено | `data/processed/ef_dynamics_rolling_252d.pkl` |
| 9b* | Динамика EF, расширяющееся окно | Выполнено | `data/processed/ef_dynamics_expanding.pkl` |
| 10* | Динамика EF, EWMA (три lambda) | Выполнено | `data/processed/ef_dynamics_ewma_*.pkl` |
| 11 | Выбор индекса для beta (IMOEX) | Выполнено | `data/processed/beta_historical.parquet` |
| 12 | Выбор окна для beta (252d, OLS) | Выполнено | `data/processed/beta_adjusted.parquet` |
| 13 | Sigma на основе historical beta | Выполнено | `data/processed/beta_cov_matrix.parquet` |
| 14 | EF на Sigma_beta | Выполнено | `data/processed/ef_beta_*.parquet` |
| 15* | Динамика EF на beta для разных окон | Выполнено | `data/processed/ef_dynamics_beta_252d.pkl` |
| 16 | Sigma на основе adjusted beta | Не начато | -- |
| 17 | EF на Sigma_adj | Не начато | -- |
| 18* | Динамика EF на adjusted beta | Не начато | -- |
| 19 | Сравнение трёх подходов (hist, beta, adj) | Не начато | -- |
| 20* | Сравнение для разных окон | Не начато | -- |
| 21* | Все пункты со звёздочкой | Частично | 2b, 3, 9b, 10, 15 сделаны; осталось 18 и 20 |
| 22** | Black's two-fund theorem | Не начато | -- |
| 23*** | Monte Carlo frontier | Не начато | -- |
| 24**** | Maximum risk portfolio | Не начато | -- |
| 25***** | Implementation shortfall | Не начато | -- |

---

## Ключевые результаты (задачи 1--15)

### Данные (задача 1)

- 30 акций, 82 670 наблюдений OHLCV, 2612 общих торговых дней
- Общий период: с 2015-07-02 (первый полный день CBOM) по 2025-12-30
- 3 корпоративных действия скорректированы: сплиты GMKN (1:100) и PLZL (1:10), консолидация VTBR (5000:1)
- 6 замен тикеров с неполной историей (см. таблицу выше)
- Источники: MOEX ISS API (через библиотеку `apimoex`), ЦБ РФ (ключевая ставка)
- Перекрёстная проверка: данные сверены с Finam

### Ковариационные матрицы (задачи 2--3)

Рассчитаны простые дневные доходности (2611 дней x 30 тикеров) и серии ковариационных матриц 30x30 семью методами:

| Метод | Файл | Особенности |
|-------|------|-------------|
| Rolling 252d | `rolling_252d_covs.pkl` | Основной, 1 год, ~10 матриц |
| Rolling 63d | `rolling_63d_covs.pkl` | Квартальное окно |
| Rolling 21d | `rolling_21d_covs.pkl` | Месячное окно, Ledoit-Wolf shrinkage (n/p = 0.7, сингулярность) |
| Expanding window | `expanding_covs.pkl` | 11 точек, шаг ~1 год |
| EWMA lambda=0.94 | `ewma_094_covs.pkl` | Быстрое затухание |
| EWMA lambda=0.97 | `ewma_097_covs.pkl` | Компромисс |
| EWMA lambda=0.99 | `ewma_099_covs.pkl` | Медленное затухание |

Все матрицы проверены на PSD (positive semi-definite), condition number, корреляции в диапазоне [-1, 1]. Всего 154 теста.

### Efficient Frontier -- 4 режима ограничений (задачи 4--8)

Выбрано окно: rolling 252d, последний торговый год, конец 30.12.2025.
Безрисковая ставка: rf = 16% (ключевая ставка ЦБ на конец 2025).
Оптимизатор: scipy SLSQP, ftol=1e-12, maxiter 1000--2000.

| Режим | GMVP Return | GMVP Std | Sharpe (GMVP) |
|-------|-------------|----------|---------------|
| Unrestricted | -9.4% | 13.6% | -1.86 |
| Short <= 25% | -9.4% | 13.6% | -1.86 |
| Long only | 1.2% | 17.4% | -0.85 |
| Min 2% | -0.2% | 20.6% | -0.78 |

Sharpe ratio отрицательный во всех режимах -- типичная ситуация для российского рынка 2025 года, когда ключевая ставка (16%) выше доходности GMVP. Фактически: зачем рисковать в акциях, если безрисковая ставка и так высокая.

Вложенность границ: min_std(unrestricted) <= min_std(short_25) <= min_std(long_only) <= min_std(min_2pct) -- чем жёстче ограничения, тем выше минимальный риск.

### Динамика EF (задачи 9--10)

Построена динамика efficient frontier (unrestricted) тремя методами:

| Метод | Фронтиров | Период | Шаг |
|-------|-----------|--------|-----|
| Rolling 252d | 10 | 2016-12 -- 2025-12 | 1 год |
| Expanding window | 11 | 2016-07 -- 2025-12 | ~1 год |
| EWMA (3 lambda) | 10 x 3 | 2016-12 -- 2025-12 | 1 год |

Для каждой даты использована историческая безрисковая ставка (ключевая ставка ЦБ), а не фиксированная. Всего 61 frontier (5100 оптимизаций).

Выводы:
- Граница существенно меняется от года к году, особенно в кризисные периоды (2020, 2022)
- Expanding window даёт наиболее стабильные оценки за счёт длинной истории
- EWMA с lambda=0.97 -- компромисс между адаптивностью и стабильностью
- Высокая вариативность ставит под вопрос практическую применимость одноразовой оптимизации по Марковицу

### Beta-коэффициенты (задачи 11--12)

- Индекс для beta: IMOEX (полное покрытие 2015--2025, все 30 акций входят в состав)
- Окно: 252 дня, OLS (равные веса), конец 2025-12-30 -- согласовано с задачей 4
- beta -- от 0.46 до 1.42, среднее около 1.0
- Все 30 beta значимы (p < 0.001)
- Adjusted beta (Блюм): 2/3 * beta + 1/3 -- сжатие к 1.0

### Market Model Sigma vs Historical Sigma (задачи 13--15)

Ковариационная матрица по рыночной модели:

```
Sigma_beta = beta * beta' * sigma2_m + diag(sigma2_eps)
```

61 параметр (30 beta + 30 sigma_eps + 1 sigma_m) вместо 465 у исторической ковариации.

| Режим | Метод | GMVP Return | GMVP Std |
|-------|-------|-------------|----------|
| Unrestricted | Historical Sigma | -9.4% | 13.6% |
| Unrestricted | Market Model Sigma_beta | -8.5% | 13.0% |
| Long-only | Historical Sigma | 1.2% | 17.4% |
| Long-only | Market Model Sigma_beta | -0.1% | 17.2% |

Market model даёт чуть меньший GMVP risk за счёт фильтрации шумовых корреляций. Condition number Sigma_beta = 158.9 (ниже, чем historical 186.5). Все внедиагональные корреляции > 0 -- свойство однофакторной модели при beta > 0.

Динамика beta-based EF построена для 10 контрольных дат (rolling 252d, годовой шаг, 2016--2025).

---

## Инструкция для задач 16--25

Ниже подробные указания для каждой оставшейся задачи: что нужно сделать, какие данные использовать, какой код писать, и какие проверки провести.

### Задача 16: Ковариационная матрица на adjusted beta

Формула та же, что для historical beta (задача 13), но вместо beta_hist используем beta_adj:

```
Sigma_adj = beta_adj * beta_adj' * sigma2_m + diag(sigma2_eps)
```

Где:
- `beta_adj` -- из `data/processed/beta_adjusted.parquet`, колонка `beta_adjusted`
- `sigma2_m` -- дисперсия IMOEX за 252 дня (та же, что в задаче 13)
- `sigma2_eps` -- дисперсии остатков OLS (из `data/processed/beta_historical.parquet`, колонка `sigma_epsilon`)

Важный момент: `sigma2_eps` берём от historical regression, не пересчитываем. Adjusted beta -- это только коррекция наклона (сжатие к 1), остатки OLS не меняются.

Пример кода:

```python
import numpy as np
import pandas as pd

beta_adj_df = pd.read_parquet('data/processed/beta_adjusted.parquet')
beta_hist_df = pd.read_parquet('data/processed/beta_historical.parquet')
bench = pd.read_parquet('data/processed/benchmark_returns.parquet')
residuals = pd.read_parquet('data/processed/beta_residuals.parquet')

beta_adj = beta_adj_df['beta_adjusted'].values  # (30,)
sigma_eps = beta_hist_df['sigma_epsilon'].values  # (30,), daily std

r_m = bench.loc[residuals.index, 'return_imoex'].values
sigma2_m = np.var(r_m, ddof=1)  # daily

# аннуализация
sigma2_m_annual = sigma2_m * 252
sigma2_eps_annual = sigma_eps ** 2 * 252

# ковариационная матрица
Sigma_adj = np.outer(beta_adj, beta_adj) * sigma2_m_annual + np.diag(sigma2_eps_annual)
```

Проверки: матрица symmetric, PSD, condition number, корреляции в (0, 1).

Выходные файлы: `data/processed/beta_adj_cov_matrix.parquet` (аналог `beta_cov_matrix.parquet`).

### Задача 17: EF на Sigma_adj

Строим границу эффективных портфелей с использованием Sigma_adj из задачи 16. Модуль оптимизации уже написан: `scripts/step3_optimizer.py`.

```python
from step3_optimizer import build_efficient_frontier, find_gmvp, find_tangency

mu_df = pd.read_parquet('data/processed/selected_mu.parquet')
mu = mu_df['expected_return'].values
rf = 0.16

# unrestricted
gmvp = find_gmvp(mu, Sigma_adj, bounds=None)
mu_max = gmvp['return'] + 2 * (max(mu) - gmvp['return'])
frontier = build_efficient_frontier(mu, Sigma_adj, rf, n_points=200, bounds=None, mu_max=mu_max)

# long-only
gmvp_lo = find_gmvp(mu, Sigma_adj, bounds=[(0, 1)] * 30)
frontier_lo = build_efficient_frontier(mu, Sigma_adj, rf, n_points=200, bounds=[(0, 1)] * 30, mu_max=max(mu))
```

Замечание про unrestricted: `mu_max` задаётся вручную (GMVP + 2 * (max(mu) - GMVP)), иначе optimizer расходится на больших целевых доходностях.

Выходные файлы: `data/processed/ef_adj_unrestricted.parquet`, `data/processed/ef_adj_long_only.parquet`, веса в pickle.

### Задача 18* (бонусная): Динамика EF на adjusted beta

Аналог задачи 15. Для каждой из ~10 контрольных дат:

1. Взять 252 дневных доходности акций и рынка
2. OLS-регрессии -> beta_hist -> beta_adj = 2/3 * beta_hist + 1/3
3. sigma2_m и sigma2_eps из той же регрессии
4. Sigma_adj = beta_adj * beta_adj' * sigma2_m * 252 + diag(sigma2_eps * 252)
5. Построить EF unrestricted

Контрольные даты можно взять из `ef_dynamics_beta_252d.pkl['dates']` -- те же даты, что в задачах 9, 10, 15.

За основу кода можно взять `scripts/step6_beta_dynamics.py`, заменив historical beta на adjusted.

Выходные файлы: `data/processed/ef_dynamics_adj_beta_252d.pkl`.

### Задача 19: Сравнение трёх подходов

Наложить три frontier (unrestricted) на одном графике:

1. **Historical Sigma** -- из `data/processed/ef_unrestricted.parquet`
2. **Market Model (hist beta)** -- из `data/processed/ef_beta_unrestricted.parquet`
3. **Market Model (adj beta)** -- результат задачи 17

Нужно:
- График: три кривые в координатах (std, return), общая ось
- Таблица ключевых портфелей (GMVP, tangency, equal-weight) x 3 метода
- Аналогичный график и таблица для long-only

Экономическая интерпретация (на что обратить внимание):
- Historical Sigma: 465 параметров, включает шумовые корреляции, может переподгонять
- Hist beta Sigma: 61 параметр, все корреляции задаются через рынок, фильтрация шума
- Adj beta Sigma: то же + beta сжаты к 1 (mean reversion), более стабильная оценка

Вопросы для обсуждения: какой метод даёт самый низкий GMVP risk? Насколько различаются веса GMVP? Какой метод предпочтительнее для практического использования?

Выходные файлы: `data/processed/ef_comparison_three_methods.parquet`, графики в `temp/`.

### Задача 20* (бонусная): Сравнение для разных окон

Три серии фронтиров по годам:
- Historical: `data/processed/ef_dynamics_rolling_252d.pkl`
- Hist beta: `data/processed/ef_dynamics_beta_252d.pkl`
- Adj beta: результат задачи 18

Построить:
- GMVP trajectory (GMVP risk vs time) для трёх методов на одном графике
- GMVP return trajectory
- По желанию -- анимация или серия фронтиров для одной выбранной даты

Экономическая интерпретация: какой метод даёт наиболее стабильные оценки GMVP? Как ведут себя три подхода в кризисные периоды (2020, 2022)?

### Задача 21* (бонусная): Все задачи со звёздочкой

Задачи 2b, 3, 9b, 10, 15 уже выполнены. Для полного зачёта осталось выполнить задачи 18 и 20.

### Задача 22** (бонусная): Black's two-fund theorem

Black (1972): любой портфель на MVF (minimum variance frontier) -- линейная комбинация двух других портфелей на этой границе, если short sales разрешены.

Как проверить:
1. Взять два произвольных портфеля P1 и P2 с frontier из `ef_unrestricted.parquet` (например, GMVP и tangency)
2. Для третьего портфеля P3 с frontier найти alpha такое, что w3 = alpha * w1 + (1 - alpha) * w2
3. Проверить, что alpha * w1 + (1 - alpha) * w2 совпадает с w3 по весам (с точностью до optimizer)
4. Повторить для нескольких P3

Веса портфелей: `data/processed/ef_unrestricted_weights.pkl`.

Ожидаемый результат: отклонение весов < 1e-4 для всех точек на frontier.

### Задача 23*** (бонусная): Monte Carlo frontier

1. Сгенерировать N = 10 000 случайных портфелей (random seed = 42):
   - Веса: np.random.dirichlet(np.ones(30)) -- равномерные на симплексе, sum = 1, все >= 0
2. Для каждого портфеля рассчитать (mu, sigma) по тем же mu и Sigma, что в задаче 4
3. Scatter plot: (std, return), 10 000 точек
4. Наложить аналитическую EF long-only из `ef_long_only.parquet`
5. Найти Monte Carlo GMVP (точка с min std) и сравнить с аналитическим

Ожидаемый результат: облако точек, аналитическая EF -- верхняя граница облака. Monte Carlo GMVP хуже аналитического (Monte Carlo не ищет оптимум, а семплирует).

### Задача 24**** (бонусная): Maximum risk portfolio

Задача, обратная GMVP: maximize w' * Sigma * w при sum(w) = 1.

```python
from scipy.optimize import minimize

def neg_variance(w, cov):
    return -w @ cov @ w

result = minimize(neg_variance, x0=np.ones(30) / 30, args=(Sigma,),
                  method='SLSQP',
                  constraints={'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
```

Без ограничений на short sales -- портфель может уходить в экстремальные веса. Строим границу наиболее рискованных портфелей (maximum risk frontier): для каждого целевого return находим портфель с максимальной дисперсией.

### Задача 25***** (бонусная): Implementation shortfall

Встроить транзакционные издержки в оптимизацию:

```
maximize Sharpe(w) - lambda * TC(w)
```

где TC(w) учитывает:
- Bid-ask spread (можно оценить по OHLCV как (high - low) / close)
- Market impact (пропорционален объёму позиции)
- lambda -- параметр штрафа

Задача исследовательская: показать, как оптимальный портфель меняется при разных lambda. При lambda = 0 получаем обычный Markowitz, при lambda -> infinity все веса стремятся к текущему портфелю (не торговать).

---

## Структура файлов

### Данные

```
data/
  raw/                              # сырые данные с MOEX ISS API
    ohlcv_daily.parquet             # дневные котировки 30 акций (OHLCV), long format
    benchmark_daily.parquet         # индекс IMOEX
    risk_free_rate.parquet          # ключевая ставка ЦБ РФ
  processed/                        # очищенные и расчётные данные
    prices_adjusted.parquet         # цены закрытия, скорректированные на сплиты
    prices_common_wide.parquet      # цены в wide формате (дата x тикер)
    returns_daily.parquet           # простые дневные доходности (2611 x 30)
    returns_daily_log.parquet       # лог-доходности
    benchmark_returns.parquet       # дневные доходности IMOEX
    selected_mu.parquet             # ожидаемые доходности (252d rolling)
    selected_cov.parquet            # ковариационная матрица (252d rolling)
    selected_rf.parquet             # безрисковая ставка
    rolling_*_covs.pkl              # скользящие ковариационные матрицы (252d, 63d, 21d)
    expanding_covs.pkl              # расширяющееся окно
    ewma_*_covs.pkl                 # EWMA (lambda = 0.94, 0.97, 0.99)
    ef_unrestricted.parquet         # EF без ограничений (200 точек)
    ef_short_25.parquet             # EF short <= 25%
    ef_long_only.parquet            # EF long only
    ef_min_2pct.parquet             # EF min 2%
    ef_*_weights.pkl                # веса портфелей на EF
    ef_portfolios.pkl               # сводный файл всех EF
    ef_dynamics_*.pkl               # динамика EF (rolling, expanding, ewma)
    beta_historical.parquet         # historical beta (OLS)
    beta_adjusted.parquet           # adjusted beta (Блюм)
    beta_residuals.parquet          # остатки OLS-регрессий (252 x 30)
    beta_regression_stats.parquet   # R2, t-stat, p-value
    beta_cov_matrix.parquet         # Sigma_beta (market model)
    beta_cov_decomposition.pkl      # разложение Sigma_beta
    ef_beta_unrestricted.parquet    # EF на Sigma_beta (unrestricted)
    ef_beta_long_only.parquet       # EF на Sigma_beta (long only)
    ef_dynamics_beta_252d.pkl       # динамика EF на beta
  meta/                             # справочники
    instruments.csv                 # тикер, компания, сектор
    trading_calendar.parquet        # торговый календарь MOEX
    corporate_actions.csv           # сплиты и консолидации
    anomalous_returns_log.csv       # обнаруженные аномальные доходности
  export/
    data_export.xlsx                # все данные в Excel (19 листов, лист spravka)
```

### Скрипты

| Скрипт | Что делает | Задача |
|--------|-----------|--------|
| `scripts/01_download_stocks.py` | Скачивает котировки 30 акций с MOEX ISS API | 1 |
| `scripts/02_download_benchmark_and_rates.py` | Скачивает IMOEX и ключевую ставку ЦБ | 1 |
| `scripts/03_corporate_actions.py` | Находит сплиты по аномальным доходностям | 1 |
| `scripts/04_validate_and_finalize.py` | Валидация, корректировка на сплиты | 1 |
| `scripts/05_replace_tickers.py` | Замена тикеров с неполной историей | 1 |
| `scripts/06_replace_lent_with_mvid.py` | Замена LENT на MVID | 1 |
| `scripts/07_visual_check.py` | Графики цен 30 акций для визуальной проверки | 1 |
| `scripts/08_finam_crosscheck.py` | Перекрёстная проверка данных с Finam | 1 |
| `scripts/09_export_excel.py` | Экспорт данных в Excel | 1 |
| `scripts/10_compute_returns.py` | Расчёт простых и лог-доходностей | 2 |
| `scripts/step2_rolling_252d.py` | Скользящее окно 252 дня | 2a |
| `scripts/step2_rolling_other.py` | Скользящие окна 63d и 21d (с shrinkage) | 2a |
| `scripts/step2_expanding.py` | Расширяющееся окно (11 точек) | 2b* |
| `scripts/step2_ewma.py` | EWMA ковариация (три lambda) | 3* |
| `scripts/step2_validate.py` | Валидация всех ковариационных матриц | 2--3 |
| `scripts/step2_visualize.py` | Визуализации: heatmap, волатильность | 2--3 |
| `scripts/step2_export_excel.py` | Экспорт доходностей и статистик в Excel | 2--3 |
| `scripts/step3_select_window.py` | Выбор исторического окна (252d rolling) | 4 |
| `scripts/step3_optimizer.py` | Модуль оптимизации по Марковицу (импортируется другими скриптами) | 4--8 |
| `scripts/step3_ef_unrestricted.py` | Граница без ограничений | 5 |
| `scripts/step3_ef_short_25.py` | Граница с short <= 25% | 6 |
| `scripts/step3_ef_long_only.py` | Граница long only | 7 |
| `scripts/step3_ef_min_2pct.py` | Граница с min 2% | 8 |
| `scripts/step3_compare_plot.py` | Сравнение четырёх границ | 5--8 |
| `scripts/step4_dynamics.py` | Модуль динамики EF (импортируется другими) | 9--10 |
| `scripts/step4_rolling.py` | Динамика EF, скользящее окно | 9a |
| `scripts/step4_expanding.py` | Динамика EF, расширяющееся окно | 9b* |
| `scripts/step4_ewma.py` | Динамика EF, EWMA | 10* |
| `scripts/step4_compare.py` | Сравнение и графики динамики | 9--10 |
| `scripts/step5_benchmark_returns.py` | Расчёт дневных доходностей IMOEX | 11 |
| `scripts/step5_select_index.py` | Выбор и обоснование индекса для beta | 11 |
| `scripts/step5_select_window.py` | Выбор окна для beta (252d, OLS) | 12 |
| `scripts/step5_compute_betas.py` | Расчёт historical и adjusted beta | 11--12 |
| `scripts/step5_export_excel.py` | Экспорт beta в Excel | 11--12 |
| `scripts/step6_beta_cov.py` | Ковариационная матрица на historical beta | 13 |
| `scripts/step6_ef_beta.py` | EF на Sigma_beta | 14 |
| `scripts/step6_beta_dynamics.py` | Динамика EF на beta | 15* |
| `scripts/step6_compare_plot.py` | Сравнение EF: historical vs beta | 13--14 |
| `scripts/step6_export_excel.py` | Экспорт в Excel | 13--15 |

### Тесты

| Файл | Что проверяет |
|------|---------------|
| `tests/test_step1_data.py` | Целостность данных (30 тикеров, сплиты, пропуски) |
| `tests/test_step2_data.py` | Ковариационные матрицы (PSD, размеры, condition number) |
| `tests/test_step2_returns.py` | Доходности (shape, NaN, диапазон) |
| `tests/test_step3.py` | EF (монотонность, GMVP, Sharpe, вложенность) |
| `tests/test_step3_stage1.py` | Выбор окна и входные данные для EF |
| `tests/test_step4.py` | Файлы динамики EF |
| `tests/test_step4_dynamics.py` | Содержимое pickle с динамикой |
| `tests/test_step5.py` | Beta (значимость, диапазон, adjusted) |
| `tests/test_step6_phase_a.py` | Sigma_beta (PSD, condition number) |
| `tests/test_step6_phase_b.py` | EF на beta (монотонность, ключевые портфели) |
| `tests/test_step6_phase_c.py` | Динамика EF на beta |

Запуск всех тестов:

```bash
pytest tests/ -v
```

### Остальное

| Файл | Назначение |
|------|-----------|
| `task.txt` | Полный текст задания (25 задач) |
| `tickers_30.md` | Обоснование выбора 30 тикеров и замен |
| `requirements.txt` | Зависимости Python |
| `CLAUDE.md` | Инструкции для AI-помощника (технические решения, стиль) |
| `step_1.txt` ... `step_6.txt` | Планы выполнения по этапам |
| `temp/` | Промежуточные графики и логи проверок |

---

## Технические параметры

| Параметр | Значение |
|----------|----------|
| Период данных | 01.01.2015 -- 31.12.2025 |
| Общий период (все 30 акций) | 02.07.2015 -- 30.12.2025 |
| Торговых дней | 2612 |
| Доходностей | 2611 (простые, не лог) |
| Аннуализация доходности и ковариации | x 252 |
| Аннуализация волатильности | x sqrt(252) |
| Безрисковая ставка | 16% (ключевая ставка ЦБ, конец 2025) |
| Optimizer | scipy.optimize.minimize, метод SLSQP |
| ftol | 1e-12 |
| maxiter | 1000--2000 |
| Random seed | 42 |
| Формат хранения | Parquet (long format), CSV для справочников |
| 3D ковариационные матрицы | pickle (dict: dates, tickers, covs) |

---

## Источники данных

- **MOEX ISS API** (`iss.moex.com`) -- котировки акций и индекса IMOEX, через библиотеку `apimoex`
- **ЦБ РФ** (`cbr.ru/DailyInfo.asmx`) -- ключевая ставка (безрисковая ставка)
- **Finam** (`finam.ru`) -- перекрёстная проверка котировок

Цепочка данных: MOEX ISS API <- Московская биржа <- торги на MOEX <- заявки брокеров. Ключи доступа не требуются.

---

## Зависимости

```
apimoex==1.3.0          # доступ к MOEX ISS API
requests==2.32.3        # HTTP-запросы
pandas==2.2.3           # основной инструмент работы с данными
numpy==1.26.4           # линейная алгебра, матрицы
matplotlib==3.9.3       # графики
seaborn==0.13.2         # тепловые карты корреляций
pyarrow==18.1.0         # чтение/запись Parquet
openpyxl==3.1.5         # экспорт в Excel
beautifulsoup4==4.12.3  # парсинг HTML (Finam)
lxml==5.3.0             # парсинг XML (ЦБ SOAP API)
pytest==8.3.4           # тестирование
```

Также используется `scipy` (входит в стандартную среду Colab/Anaconda) и `statsmodels` (для OLS-регрессий).

---

## Известные особенности данных

1. **Период 02--03.2022 (приостановка торгов на MOEX):** торги были остановлены на несколько недель. Данные за этот период отсутствуют, пропуск остаётся в данных. При расчёте доходностей за границей перерыва получается один аномальный наблюдение -- оно входит в выборку, описано в методологии.

2. **21-дневное скользящее окно (n/p = 0.7):** ковариационная матрица сингулярна (30 акций, 21 наблюдение). Обязательно использовать Ledoit-Wolf shrinkage.

3. **CBOM (МКБ):** торги начались 02.07.2015, поэтому общий период всех 30 акций начинается с этой даты (а не с 01.01.2015).

4. **Отрицательный Sharpe ratio:** при rf = 16% доходность GMVP ниже безрисковой ставки во всех режимах. Это не ошибка -- это реальная ситуация на российском рынке конца 2025 года.

5. **Short_25 frontier:** при визуализации уходит далеко вправо (std до 230%), при построении графиков нужно обрезать ось X.
