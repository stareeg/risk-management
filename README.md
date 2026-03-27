# Управление портфелем: эффективные портфели на российских акциях

Студенческий проект по курсу "Управление портфелем" (магистратура). Применяем теорию Марковица на реальных данных 30 российских акций с Московской биржи за 2015--2025 годы: строим границы эффективных портфелей при различных ограничениях, считаем бета-коэффициенты по рыночной модели, сравниваем три подхода к оценке ковариационной матрицы (историческая, на основе historical beta, на основе adjusted beta).

Задание содержит 25 задач. Выполнены задачи 1--20 (включая бонусные). Остаются задачи 21--25, инструкции к ним подробно описаны ниже.

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
| 16 | Sigma на основе adjusted beta | Выполнено | `data/processed/adj_beta_cov_matrix.parquet` |
| 17 | EF на Sigma_adj | Выполнено | `data/processed/ef_adj_beta_*.parquet` |
| 18* | Динамика EF на adjusted beta | Выполнено | `data/processed/ef_dynamics_adj_beta_252d.pkl` |
| 19 | Сравнение трёх подходов (hist, beta, adj) | Выполнено | `data/processed/step8_comparison_static.parquet` |
| 20* | Сравнение для разных окон | Выполнено | `data/processed/step8_dynamics_comparison.parquet` |
| 21* | Все пункты со звёздочкой | Выполнено | 2b, 3, 9b, 10, 15, 18, 20 -- все (*) из пп. 2--20 |
| 22** | Black's two-fund theorem | Не начато | -- |
| 23*** | Monte Carlo frontier | Не начато | -- |
| 24**** | Maximum risk portfolio | Не начато | -- |
| 25***** | Implementation shortfall | Не начато | -- |

---

## Ключевые результаты (задачи 1--18)

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

### Adjusted Beta Sigma vs Historical и Market Model (задачи 16--18)

Ковариационная матрица на adjusted beta (формула Блюма: beta_adj = 2/3 * beta_hist + 1/3):

```
Sigma_adj = beta_adj * beta_adj' * sigma2_m + diag(sigma2_eps)
```

Adjusted beta сжаты к 1.0, что делает систематический компонент более однородным. Остаточные дисперсии (sigma2_eps) те же, что в step 6 -- adjusted beta корректирует только наклон регрессии.

| Режим | Метод | GMVP Return | GMVP Std | Condition # |
|-------|-------|-------------|----------|-------------|
| Unrestricted | Historical Sigma | -9.4% | 13.6% | 186.5 |
| Unrestricted | Market Model Sigma_beta | -8.5% | 13.0% | 158.9 |
| Unrestricted | Adjusted Beta Sigma_adj | -9.1% | 16.6% | 152.4 |
| Long-only | Historical Sigma | 1.2% | 17.4% | -- |
| Long-only | Market Model Sigma_beta | -0.1% | 17.2% | -- |
| Long-only | Adjusted Beta Sigma_adj | -0.3% | 19.9% | -- |

Condition number Sigma_adj = 152.4 -- наименьший среди трёх методов, что подтверждает более однородную структуру. GMVP std для Sigma_adj выше, чем для двух других методов: сжатие beta к 1.0 убирает «экстремальные» оптимизационные решения, но поднимает нижнюю границу волатильности.

Динамика adjusted-beta EF построена для тех же 10 контрольных дат. Adjusted beta стабильнее во времени за счёт регрессии к среднему.

### Сравнение трёх подходов к оценке Sigma (задачи 19--20)

Три метода оценки ковариационной матрицы формально сравнены на выбранном окне (rolling 252d, конец 2025-12-30) и в динамике (10 контрольных дат, 2016--2025).

**Статическое сравнение (задание 19):**

| Свойство | Historical Sigma | Market Model (beta) | Adjusted Beta |
|----------|-----------------|---------------------|---------------|
| Параметров | 465 | 61 | 61 |
| Condition number | 186.5 | 158.9 | 152.4 |
| GMVP std (unrestricted) | 13.6% | 13.0% | 16.6% |
| GMVP std (long-only) | 17.4% | 17.2% | 19.9% |
| GMVP Sharpe (unrestricted) | -1.86 | -1.88 | -1.51 |

Market model даёт наименьший GMVP risk -- меньше параметров, меньше estimation error. Adjusted beta сжимает бета к 1.0, делая акции более «похожими» -- оптимизатору сложнее найти диверсификационный выигрыш, поэтому GMVP std выше.

Все три метода дают отрицательный Sharpe ratio (rf=16%), что подтверждает доминирование безрисковых инструментов на российском рынке конца 2025 года.

**Динамическое сравнение (задание 20*):**

- Наибольшее расхождение между методами -- в кризисные периоды (COVID-2020, февраль 2022)
- Condition number: historical наименее стабилен, adjusted beta наиболее стабилен
- В спокойные периоды (2016--2019) все три метода дают похожие результаты
- Adjusted beta предпочтительнее для стратегического аллокатора (горизонт > 1 года)

---

## Инструкция для задач 16--25

Ниже подробные указания для каждой оставшейся задачи. Для каждой даём: полную формулировку из задания, входные файлы, код или подсказки, и ожидаемые выводы.

Весь код для задач 16-25 уже заготовлен в `portfolio_analysis.ipynb` (секции 7-12). Функции оптимизации встроены прямо в ноутбук, внешние модули не нужны (но если удобнее -- `scripts/step3_optimizer.py` и `scripts/step4_dynamics.py` тоже в репозитории).

---

### Задача 16: Ковариационная матрица на adjusted beta

**Формулировка из задания:** *Рассчитать на выбранном в п. 12 историческом окне для отобранных акций ковариационную матрицу на основе скорректированных beta (adjusted betas).*

**Суть:** берём ту же формулу market model, что в задаче 13, но заменяем historical beta на adjusted beta (Блюм: beta_adj = 2/3 * beta_hist + 1/3). Остатки OLS и дисперсия рынка остаются теми же -- adjusted beta корректирует только наклон регрессии.

**Входные файлы:**
| Файл | Что берём |
|------|-----------|
| `data/processed/beta_adjusted.parquet` | beta_adj (30 значений, колонка `beta_adjusted`) |
| `data/processed/beta_historical.parquet` | sigma_epsilon (30 значений, колонка `sigma_epsilon`) -- дневное std остатков |
| `data/processed/benchmark_returns.parquet` | доходности IMOEX для расчёта sigma2_m |
| `data/processed/beta_residuals.parquet` | индекс (252 даты) -- для выравнивания окна с benchmark |

**Формула:**
```
Sigma_adj = beta_adj * beta_adj' * sigma2_m_annual + diag(sigma2_eps_annual)
```
где sigma2_m_annual = var(r_m, ddof=1) * 252, sigma2_eps_annual = sigma_eps^2 * 252.

**Код:**
```python
beta_adj_df = pd.read_parquet('data/processed/beta_adjusted.parquet')
beta_hist_df = pd.read_parquet('data/processed/beta_historical.parquet')
bench = pd.read_parquet('data/processed/benchmark_returns.parquet')
residuals = pd.read_parquet('data/processed/beta_residuals.parquet')

beta_adj = beta_adj_df['beta_adjusted'].values          # (30,)
sigma_eps = beta_hist_df['sigma_epsilon'].values         # (30,), daily std

r_m = bench.loc[residuals.index, 'return_imoex'].values  # (252,)
sigma2_m = np.var(r_m, ddof=1)                            # daily

sigma2_m_annual = sigma2_m * 252
sigma2_eps_annual = sigma_eps ** 2 * 252

Sigma_adj = np.outer(beta_adj, beta_adj) * sigma2_m_annual + np.diag(sigma2_eps_annual)
```

**Проверки:** symmetric, PSD (eigenvalues >= 0), condition number, корреляции в (0, 1), все диагональные > 0.

**Ожидаемые выводы:**
- Sigma_adj будет ближе к Sigma_beta (historical beta), чем к исторической Sigma -- разница только в значениях beta
- Adjusted beta сжаты к 1.0, поэтому внедиагональные элементы Sigma_adj более однородные (корреляции ближе друг к другу)
- Condition number Sigma_adj должен быть сопоставим с Sigma_beta (~159), возможно чуть ниже за счёт более однородных beta
- Все корреляции > 0 (как и в задаче 13 -- свойство однофакторной модели)

**Выходные файлы:** `data/processed/beta_adj_cov_matrix.parquet`

---

### Задача 17: EF на Sigma_adj

**Формулировка из задания:** *Построить границу эффективных портфелей на основе полученной в п. 16 ковариационной матрицы.*

**Суть:** строим efficient frontier точно так же, как в задаче 14, но подставляем Sigma_adj вместо Sigma_beta. Вектор ожидаемых доходностей mu тот же (свойство OLS: среднее доходности совпадает с предсказанием модели).

**Входные файлы:**
| Файл | Что берём |
|------|-----------|
| Sigma_adj из задачи 16 | ковариационная матрица 30x30 |
| `data/processed/selected_mu.parquet` | вектор mu (30 акций, колонка `expected_return`) |
| `data/processed/selected_rf.parquet` | rf = 16% (колонка `rf_annual`) |

**Код:**
```python
mu = pd.read_parquet('data/processed/selected_mu.parquet')['expected_return'].values
rf = 0.16

# unrestricted
gmvp = find_gmvp(mu, Sigma_adj, bounds=None)
mu_max = gmvp['return'] + 2 * (max(mu) - gmvp['return'])
frontier_unr = build_efficient_frontier(mu, Sigma_adj, rf, n_points=200, bounds=None, mu_max=mu_max)

# long-only
gmvp_lo = find_gmvp(mu, Sigma_adj, bounds=[(0, 1)] * 30)
frontier_lo = build_efficient_frontier(mu, Sigma_adj, rf, n_points=200, bounds=[(0, 1)] * 30, mu_max=max(mu))
```

**Ожидаемые выводы:**
- GMVP (unrestricted) будет очень близок к GMVP на Sigma_beta, потому что adjusted beta -- это линейная трансформация historical beta, а sigma_eps те же
- Небольшой сдвиг: adjusted beta ближе к 1 -> все акции «более похожи» на рынок -> диверсификация чуть хуже -> GMVP std может быть чуть выше, чем для historical beta
- Ожидаемая оценка: GMVP unrestricted return ~ -8...-9%, std ~ 13-14%
- Sharpe по-прежнему отрицательный (rf=16% > GMVP return)

**Выходные файлы:** `data/processed/ef_adj_unrestricted.parquet`, `data/processed/ef_adj_long_only.parquet`

---

### Задача 18* (бонусная): Динамика EF на adjusted beta

**Формулировка из задания:** *Построить границу эффективных портфелей для разных исторических окон и продемонстрировать динамику её изменения. Другими словами, выполнить пп. 16-17 не для одного, а для разных окон.*

**Суть:** повторяем задачу 15, но для adjusted beta. Для каждой из ~10 контрольных дат пересчитываем OLS-регрессии, получаем beta_hist, применяем формулу Блюма, строим Sigma_adj, строим EF.

**Входные файлы:**
| Файл | Что берём |
|------|-----------|
| `data/processed/returns_daily.parquet` | дневные доходности 30 акций (для OLS в каждом окне) |
| `data/processed/benchmark_returns.parquet` | доходности IMOEX (для OLS) |
| `data/processed/rolling_252d_means.parquet` | аннуализированные средние для каждого окна (mu) |
| `data/raw/risk_free_rate.parquet` | историческая ключевая ставка ЦБ |
| `data/processed/ef_dynamics_beta_252d.pkl` | только для списка контрольных дат (ключ `dates`) |

**Алгоритм для каждой контрольной даты:**
1. Взять 252 доходности акций и рынка, заканчивающиеся на эту дату
2. OLS: r_i = alpha + beta_hist * r_m + eps (statsmodels)
3. beta_adj = 2/3 * beta_hist + 1/3
4. sigma_eps = std(residuals), sigma2_m = var(r_m, ddof=1)
5. Sigma_adj = beta_adj * beta_adj' * sigma2_m * 252 + diag(sigma_eps^2 * 252)
6. mu из rolling_252d_means для этой даты (уже аннуализированные)
7. rf -- историческая ключевая ставка на эту дату
8. build_efficient_frontier(mu, Sigma_adj, rf, n_points=100, bounds=None)

**Контрольные даты** (10 штук, year-end): 2016-12-30, 2017-12-29, ..., 2025-12-30. Можно взять из `ef_dynamics_beta_252d.pkl['dates']`.

**Ожидаемые выводы:**
- Динамика adjusted-beta EF будет похожа на historical-beta EF, но более сглаженная (beta ближе к 1 -> меньше разброс корреляций между датами)
- В кризис 2022: исторические beta растут (co-movement), adjusted beta «притягиваются» к 1, сглаживая эффект -- frontier менее волатильная
- GMVP trajectory для adjusted beta должна быть более стабильной, чем для historical beta (один из ключевых выводов задачи 20)

**Выходные файлы:** `data/processed/ef_dynamics_adj_beta_252d.pkl`

---

### Задача 19: Сравнение трёх подходов

**Формулировка из задания:** *Сравнить на выбранном в п. 12 историческом окне для отобранных акций границы эффективных портфелей, рассчитанные тремя различными способами: на основе исторических доходностей, исторических и скорректированных beta. Привести экономическую интерпретацию полученных результатов.*

**Суть:** это ключевая задача проекта -- нужно наложить три frontier на один график и объяснить различия. Численные данные для двух из трёх подходов уже готовы, третий (adjusted beta) -- результат задач 16-17.

**Входные файлы:**
| Файл | Метод | Статус |
|------|-------|--------|
| `data/processed/ef_unrestricted.parquet` | Historical Sigma | Готово |
| `data/processed/ef_beta_unrestricted.parquet` | Market Model (historical beta) | Готово |
| Результат задачи 17 | Market Model (adjusted beta) | Нужно сделать |
| `data/processed/ef_long_only.parquet` | Historical Sigma, long-only | Готово |
| `data/processed/ef_beta_long_only.parquet` | Market Model (hist beta), long-only | Готово |
| Результат задачи 17 | Market Model (adj beta), long-only | Нужно сделать |
| `data/processed/ef_beta_comparison_table.parquet` | Таблица сравнения hist vs beta (образец формата) | Готово |

**Что нужно построить:**
1. График: три EF (unrestricted) в координатах (std, return), отметить GMVP и tangency для каждой
2. То же для long-only
3. Таблица ключевых портфелей: GMVP (return, std, sharpe) x 3 метода x 2 режима (unrestricted + long-only)
4. EW portfolio: return одинаковый для всех трёх (свойство OLS!), std различается

**Ожидаемые выводы:**

Три подхода к оценке Sigma:
- **Historical Sigma** (465 параметров) -- все парные корреляции оцениваются напрямую. Включает шумовые корреляции, но и реальные идиосинкратические связи между акциями (напр. PLZL как hedge).
- **Hist beta Sigma** (61 параметр) -- все корреляции только через рынок. Фильтрует шум, но игнорирует связи внутри секторов (напр. CHMF-NLMK-MAGN -- все металлурги, коррелированы сильнее, чем через рынок).
- **Adj beta Sigma** (61 параметр) -- то же, но beta сжаты к 1. Ещё более однородная структура корреляций. Mean reversion beta -- эмпирически оправданная коррекция (beta для extreme stocks возвращаются к среднему).

Границы будут расположены так:
- Все три frontiers близки друг к другу (mu одинаковый, различия только в Sigma)
- GMVP risk: скорее всего historical <= hist beta <= adj beta (или наоборот, зависит от данных)
- Market model даёт более «правильную» оценку out-of-sample (меньше overfitting), но хуже in-sample
- Для российского рынка 2025 (высокая rf) практическая разница минимальна -- все подходы дают отрицательный Sharpe

---

### Задача 20* (бонусная): Сравнение для разных окон

**Формулировка из задания:** *Выполнить п. 19 для разных окон. Привести экономическую интерпретацию полученных результатов.*

**Суть:** для каждой из ~10 контрольных дат (2016-2025) сравниваем три подхода. Основная визуализация -- GMVP trajectory (как GMVP risk меняется во времени для каждого метода).

**Входные файлы:**
| Файл | Метод | Статус |
|------|-------|--------|
| `data/processed/ef_dynamics_rolling_252d.pkl` | Historical Sigma | Готово |
| `data/processed/ef_dynamics_beta_252d.pkl` | Hist beta | Готово |
| Результат задачи 18 | Adj beta | Нужно сделать |

Из каждого pkl берём `key_portfolios` -> `gmvp` -> `std` и `return` для каждой даты.

**Что построить:**
1. GMVP std vs time (три линии на одном графике)
2. GMVP return vs time (три линии)
3. Таблица: mean и std GMVP risk по 10 датам для каждого метода

**Ожидаемые выводы:**
- Historical Sigma даёт самый нестабильный GMVP (высокая дисперсия risk от года к году), потому что 465 параметров шумят
- Market model (hist beta) более стабилен -- 61 параметр, ковариационная матрица структурирована
- Adjusted beta -- самый стабильный из трёх: beta сжаты к 1, меньше разброс между датами
- В кризис (2022): historical GMVP risk резко растёт (все корреляции идут к 1), market model растёт меньше, adjusted beta -- ещё меньше
- Вывод для практики: market model с adjusted beta -- предпочтительный подход для стабильного портфельного управления. Historical Sigma подходит для краткосрочного тактического allocation

---

### Задача 21* (бонусная): Все задачи со звёздочкой

**Формулировка из задания:** *Выполнить пункты с (\*), которая встречается в пп. 2-20.*

**Статус:** задачи 2b, 3, 9b, 10, 15 уже выполнены. Для полного зачёта осталось выполнить задачи 18 и 20 (см. выше).

---

### Задача 22** (бонусная): Black's two-fund theorem

**Формулировка из задания:** *Проверить Black's (1972) two-fund theorem, согласно которой все портфели на границе портфелей с минимальной дисперсией являются линейной комбинацией любых двух других портфелей на этой границе при условии, что короткие продажи разрешены.*

**Суть:** математически строгое свойство MVF (minimum variance frontier) для unrestricted case. Берём два «опорных» портфеля -- GMVP и tangency -- и показываем, что любой третий портфель на frontier можно точно выразить через них.

**Входные файлы:**
| Файл | Что берём |
|------|-----------|
| `data/processed/ef_unrestricted.parquet` | frontier (200 точек: target_return, portfolio_return, portfolio_std) |
| `data/processed/selected_mu.parquet` | mu (для пересчёта return/std из весов) |
| `data/processed/selected_cov.parquet` | Sigma (для пересчёта return/std из весов) |

Веса всех 200 портфелей хранятся в `ef_unrestricted_weights.pkl` -> `frontier_weights` (200, 30). GMVP: ключ `gmvp` -> `weights`, tangency: ключ `tangency` -> `weights`. Но этого файла нет в репозитории -- пересчитайте GMVP и tangency из `selected_mu` + `selected_cov` функциями `find_gmvp` и `find_tangency` из ноутбука.

**Алгоритм:**
```python
w1 = find_gmvp(mu, Sigma)['weights']              # P1 = GMVP
w2 = find_tangency(mu, Sigma, rf)['weights']       # P2 = Tangency

# для каждого портфеля P3 на frontier подбираем alpha:
# w3 = alpha * w1 + (1 - alpha) * w2
# => alpha = (w3 - w2) / (w1 - w2), по любой компоненте
# или через least squares: alpha = argmin || w3 - alpha*w1 - (1-alpha)*w2 ||

for i, w3 in enumerate(frontier_weights):
    # alpha через return:
    r1 = w1 @ mu; r2 = w2 @ mu; r3 = w3 @ mu
    if abs(r1 - r2) > 1e-10:
        alpha = (r3 - r2) / (r1 - r2)
        w_reconstructed = alpha * w1 + (1 - alpha) * w2
        error = np.max(np.abs(w3 - w_reconstructed))
```

**Ожидаемые выводы:**
- Для unrestricted case: ошибка реконструкции < 1e-3 для всех 200 точек (ограничена только точностью optimizer)
- Теорема подтверждается: MVF -- линейное подпространство в пространстве весов
- Для long-only: теорема НЕ выполняется (ограничения нарушают линейность -- покажите это для сравнения)
- Практический смысл: инвестору достаточно двух ETF (GMVP-like и aggressive) для любой позиции на frontier

---

### Задача 23*** (бонусная): Monte Carlo frontier

**Формулировка из задания:** *Рассчитать границу эффективных портфелей (для отобранных акций) с помощью метода статистических испытаний (Монте-Карло).*

**Суть:** вместо аналитической оптимизации генерируем тысячи случайных портфелей и смотрим, как они заполняют пространство (risk, return). Аналитическая EF -- верхняя граница этого облака.

**Входные файлы:**
| Файл | Что берём |
|------|-----------|
| `data/processed/selected_mu.parquet` | mu (30 акций) |
| `data/processed/selected_cov.parquet` | Sigma (30x30) |
| `data/processed/ef_long_only.parquet` | аналитическая EF long-only (для наложения) |

**Код:**
```python
np.random.seed(42)
n_simulations = 10000

# случайные веса на симплексе (long-only, sum=1)
mc_weights = np.random.dirichlet(np.ones(30), size=n_simulations)

mc_returns = mc_weights @ mu
mc_stds = np.array([np.sqrt(w @ Sigma @ w) for w in mc_weights])

# scatter plot
plt.scatter(mc_stds, mc_returns, s=1, alpha=0.3, label='Monte Carlo (10000)')
plt.plot(ef_lo['portfolio_std'], ef_lo['portfolio_return'], 'r-', lw=2, label='Analytical EF')
```

**Ожидаемые выводы:**
- Облако точек формирует характерную «пулю» (bullet shape) в координатах (std, return)
- Аналитическая EF long-only -- это именно верхняя граница облака (efficient set)
- Нижняя граница облака -- «anti-efficient» frontier (максимальный risk для данного return)
- Monte Carlo GMVP (точка с min std среди 10000) хуже аналитического GMVP: ~18-20% vs 17.4% -- потому что случайный поиск не находит точный оптимум
- Чем больше N, тем ближе MC-граница к аналитической, но сходимость медленная (проклятие размерности: 30 акций)
- Практический вывод: Monte Carlo -- визуально наглядный метод, но для реальной оптимизации бесполезен; аналитический подход строго лучше

---

### Задача 24**** (бонусная): Maximum risk portfolio

**Формулировка из задания:** *Найти структуру наиболее рискованного портфеля на основе собранных данных (при отсутствии ограничений на короткие продажи и вложения в отдельные акции). Построить границу наиболее рискованных портфелей.*

**Суть:** задача, обратная GMVP. Вместо min w'Sigma*w решаем max w'Sigma*w при sum(w)=1. Без ограничений на short sales портфель может набирать экстремальные веса.

**Входные файлы:** `selected_mu.parquet`, `selected_cov.parquet`

**Код:**
```python
from scipy.optimize import minimize

def neg_variance(w, cov):
    return -w @ cov @ w

# максимально рискованный портфель (unrestricted)
result = minimize(neg_variance, x0=np.ones(30) / 30, args=(Sigma,),
                  method='SLSQP',
                  constraints={'type': 'eq', 'fun': lambda w: np.sum(w) - 1})
max_risk_weights = result.x

# граница наиболее рискованных портфелей: для каждого target return
# находим портфель с МАКСИМАЛЬНОЙ дисперсией
target_returns = np.linspace(min(mu) - 0.5, max(mu) + 0.5, 200)
max_risk_frontier = []
for target in target_returns:
    res = minimize(neg_variance, x0=np.ones(30)/30, args=(Sigma,),
                   method='SLSQP',
                   constraints=[
                       {'type': 'eq', 'fun': lambda w: np.sum(w) - 1},
                       {'type': 'eq', 'fun': lambda w, t=target: w @ mu - t}
                   ])
    if res.success:
        max_risk_frontier.append({'return': target, 'std': np.sqrt(-res.fun)})
```

**Ожидаемые выводы:**
- Максимально рискованный портфель будет содержать экстремальные short/long позиции (веса >> 1 и << -1)
- Maximum risk frontier -- нижняя граница feasible region в координатах (std, return) для unrestricted case
- Вместе с EF (верхняя граница) они образуют полную MVF (minimum variance frontier) -- «гиперболу» Марковица
- Std максимально рискованного портфеля может достигать сотен процентов (за счёт leverage через short sales)
- Для long-only: max risk портфель = 100% в самую волатильную акцию (тривиально)

---

### Задача 25***** (бонусная): Implementation shortfall

**Формулировка из задания:** *Встроить Implementation Shortfall, рассчитанный для дневных цен закрытия, в оптимизационную задачу (portfolio optimizer).*

**Суть:** Markowitz не учитывает транзакционные издержки. В реальности при ребалансировке портфеля приходится платить спред и двигать рынок. Implementation shortfall -- это разница между «бумажной» доходностью оптимального портфеля и реальной доходностью после учёта всех издержек.

**Входные файлы:** `selected_mu.parquet`, `selected_cov.parquet`, `data/raw/ohlcv_daily.parquet` (для оценки спредов из OHLCV)

**Подход:**
```
maximize: w'*mu - rf - lambda * TC(w)  (или maximize Sharpe - lambda * TC)
```
где TC(w) = sum_i |w_i - w_i_current| * c_i, c_i -- транзакционные издержки для акции i.

Оценка c_i: bid-ask spread ~ (high - low) / close, усреднённый за окно. Более точно: market impact ~ sqrt(|delta_w_i| / ADV_i), где ADV -- средний дневной объём.

**Ожидаемые выводы:**
- При lambda=0 получаем обычный Markowitz (нет штрафа за торговлю)
- При lambda -> inf оптимальный портфель не торгуется (все веса -> текущий портфель)
- Для промежуточных lambda: портфель «регуляризуется» -- меньше экстремальных весов, меньше turnover
- TC-adjusted GMVP будет иметь чуть больший risk, но значительно меньший turnover
- Для российского рынка: спреды голубых фишек (SBER, GAZP) малы (~0.1%), для менее ликвидных акций (MVID, FEES) могут достигать 0.5-1%
- Практический вывод: учёт TC делает market model (задачи 13-14) более привлекательным -- его веса стабильнее, а значит, turnover ниже

---

## Структура репозитория

В репозитории только файлы, нужные для продолжения работы. Промежуточные скрипты, тесты и временные файлы не включены -- результаты их работы уже в data/.

```
portfolio_analysis.ipynb              # основной ноутбук (задачи 1-15 + заготовки 16-25)
README.md                             # этот файл
requirements.txt                      # зависимости Python

scripts/
  step3_optimizer.py                  # модуль оптимизации по Марковицу
  step4_dynamics.py                   # модуль динамики EF

data/
  raw/
    risk_free_rate.parquet            # ключевая ставка ЦБ РФ (2015-2025)
  processed/
    # --- исходные данные ---
    prices_adjusted.parquet           # цены закрытия 30 акций, скорректированные на сплиты
    returns_daily.parquet             # простые дневные доходности (2611 x 30)
    benchmark_returns.parquet         # дневные доходности IMOEX

    # --- выбранное окно (задача 4): rolling 252d, конец 2025-12-30 ---
    selected_mu.parquet               # вектор ожидаемых доходностей mu (30 акций)
    selected_cov.parquet              # историческая ковариационная матрица 30x30
    selected_rf.parquet               # безрисковая ставка (rf = 16%)

    # --- EF на исторической Sigma (задачи 5-8): 4 режима ограничений ---
    ef_unrestricted.parquet           # EF без ограничений (200 точек)
    ef_short_25.parquet               # EF short <= 25% (200 точек)
    ef_long_only.parquet              # EF long only (200 точек)
    ef_min_2pct.parquet               # EF min 2% в каждую акцию (200 точек)
    ef_portfolios.pkl                 # сводка: GMVP, tangency, EW для всех 4 режимов
    ef_unrestricted_weights.pkl       # веса 200 портфелей на unrestricted frontier
    ef_long_only_weights.pkl          # веса портфелей на long-only frontier

    # --- динамика EF (задачи 9-10): серии фронтиров по годам ---
    rolling_252d_means.parquet        # скользящие средние доходности (для пересчёта mu)
    rolling_252d_covs.pkl             # скользящие ковариационные матрицы (17 MB)
    ef_dynamics_rolling_252d.pkl      # 10 фронтиров, rolling 252d, year-end
    ef_dynamics_expanding.pkl         # 11 фронтиров, расширяющееся окно

    # --- бета-коэффициенты (задачи 11-12) ---
    beta_historical.parquet           # historical beta + sigma_epsilon (OLS, 30 акций)
    beta_adjusted.parquet             # adjusted beta (Блюм: 2/3*beta + 1/3)
    beta_residuals.parquet            # остатки OLS-регрессий (252 x 30)
    beta_dynamics_rolling_252d.pkl    # бета по 10 контрольным датам (для задачи 18)

    # --- EF на market model Sigma_beta (задачи 13-15) ---
    beta_cov_matrix.parquet           # Sigma_beta = beta*beta'*sigma2_m + diag(sigma2_eps)
    ef_beta_unrestricted.parquet      # EF unrestricted на Sigma_beta (200 точек)
    ef_beta_long_only.parquet         # EF long-only на Sigma_beta
    ef_beta_weights.pkl               # веса портфелей на beta-based frontier
    ef_beta_comparison_table.parquet  # таблица: GMVP/tangency/EW для hist vs beta
    ef_dynamics_beta_252d.pkl         # 10 фронтиров на beta, year-end

  meta/
    instruments.csv                   # справочник: тикер, компания, сектор
  export/
    data_export.xlsx                  # все данные в Excel (19 листов)
```

### Модули-утилиты

Два модуля в `scripts/` можно импортировать напрямую. Но все ключевые функции также встроены в секцию 3 ноутбука, так что для работы в Colab внешние модули не обязательны.

| Модуль | Ключевые функции |
|--------|------------------|
| `step3_optimizer.py` | `find_gmvp()`, `find_tangency()`, `build_efficient_frontier()`, `portfolio_return()`, `portfolio_volatility()`, `portfolio_sharpe()` |
| `step4_dynamics.py` | `subsample_dates()`, `build_frontier_series()`, `load_historical_rf()`, `get_rf_for_date()`, `plot_frontier_dynamics()` |

### Excel-файл (data_export.xlsx)

Для тех, кому удобнее работать с Excel. 19 листов:

| Лист | Содержание |
|------|-----------|
| spravka | Описание всех листов и полей |
| instruments | 30 тикеров: компания, сектор |
| ohlcv | Дневные котировки (82670 строк) |
| prices_adjusted | Скорректированные цены закрытия |
| benchmark | Индекс IMOEX |
| risk_free_rate | Ключевая ставка ЦБ |
| trading_calendar | Торговый календарь MOEX |
| corporate_actions | Сплиты и консолидации |
| returns_daily | Дневные доходности 30 акций |
| summary_statistics | Описательная статистика доходностей |
| selected_window | Параметры выбранного окна (252d) |
| ef_comparison | 4 EF frontiers (800 точек) |
| ef_key_portfolios | GMVP, tangency, EW для 4 режимов |
| ef_dynamics_summary | Метрики стабильности EF по методам |
| ef_dynamics_gmvp | Траектория GMVP по годам |
| beta_coefficients | Historical и adjusted beta, R2, сектор |
| beta_statistics | Полная OLS-диагностика (18 показателей) |
| beta_cov_matrix | Sigma_beta (market model, 30x30) |
| ef_beta_comparison | Сравнение historical vs beta EF |

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

---

## Источники данных

- **MOEX ISS API** (`iss.moex.com`) -- котировки акций и индекса IMOEX, через библиотеку `apimoex`
- **ЦБ РФ** (`cbr.ru/DailyInfo.asmx`) -- ключевая ставка (безрисковая ставка)

Ключи доступа не требуются.

---

## Зависимости

```
pandas, numpy, matplotlib, seaborn, scipy, statsmodels, pyarrow, openpyxl
```

Полный список с версиями -- в `requirements.txt`. Для Colab: `!pip install -r requirements.txt`.

---

## Известные особенности данных

1. **Период 02--03.2022 (приостановка торгов на MOEX):** торги были остановлены на несколько недель. Пропуск остаётся в данных, описан в методологии.

2. **CBOM (МКБ):** торги начались 02.07.2015, поэтому общий период всех 30 акций начинается с этой даты (а не с 01.01.2015).

3. **Отрицательный Sharpe ratio:** при rf = 16% доходность GMVP ниже безрисковой ставки во всех режимах. Это не ошибка -- это реальная ситуация для российского рынка конца 2025 года с высокой ключевой ставкой.

4. **Short_25 frontier:** при визуализации уходит далеко вправо (std до 230%), при построении графиков нужно обрезать ось X.
