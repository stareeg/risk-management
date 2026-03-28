# Управление портфелем: эффективные портфели на российских акциях

Применяем теорию Марковица на реальных данных 30 российских акций с Московской биржи за 2015--2025 годы: строим границы эффективных портфелей при различных ограничениях, считаем бета-коэффициенты по рыночной модели, сравниваем три подхода к оценке ковариационной матрицы (историческая, на основе historical beta, на основе adjusted beta), проверяем two-fund theorem Блэка.

Задание содержит 25 задач. Выполнены задачи 1--22 (включая бонусные). Остаются задачи 23--25.

---

## Быстрый старт

```bash
pip install -r requirements.txt
```

Открываем `portfolio_analysis.ipynb` и выполняем ячейки. Все данные уже лежат в `data/` -- ничего скачивать не нужно. Для Excel: сводный файл `data/export/data_export.xlsx` содержит все основные данные на 32 листах.

Если нужно воспроизвести сбор данных с нуля (MOEX ISS API + ЦБ РФ), скрипты загрузки -- в `scripts/` (01--06). Ключи доступа не требуются.

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

Замены тикеров: YNDX -> AFLT, POLY -> HYDR, TCSG -> CBOM, FIVE -> SNGSP, FLOT -> MSNG, OZON -> MVID. Причина -- отсутствие данных на MOEX за весь период 2015--2025 (IPO позже, делистинг, редомициляция).

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
| 21* | Все пункты со звёздочкой | Выполнено | все (*) из пп. 2--20 закрыты |
| 22** | Black's two-fund theorem | Выполнено | `data/processed/step10_twofund_*.parquet` |
| 23*** | Monte Carlo frontier | Не начато | -- |
| 24**** | Maximum risk portfolio | Не начато | -- |
| 25***** | Implementation shortfall | Не начато | -- |

---

## Результаты

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

Все матрицы проверены на PSD (positive semi-definite), condition number, корреляции в диапазоне [-1, 1].

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

Sharpe ratio отрицательный во всех режимах -- типичная ситуация для российского рынка 2025 года, когда ключевая ставка (16%) выше доходности GMVP.

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
- Expanding window -- наиболее стабильные оценки за счёт длинной истории
- EWMA с lambda=0.97 -- компромисс между адаптивностью и стабильностью
- Высокая вариативность ставит под вопрос практическую применимость одноразовой оптимизации по Марковицу

### Beta-коэффициенты (задачи 11--12)

- Индекс: IMOEX (полное покрытие 2015--2025, все 30 акций входят в состав)
- Окно: 252 дня, OLS (равные веса), конец 2025-12-30 -- согласовано с задачей 4
- beta от 0.46 до 1.42, среднее около 1.0
- Все 30 beta значимы (p < 0.001)
- Adjusted beta (Блюм): 2/3 * beta + 1/3 -- сжатие к 1.0

### Market Model Sigma (задачи 13--15)

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

Market model -- чуть меньший GMVP risk за счёт фильтрации шумовых корреляций. Condition number 158.9 (ниже, чем historical 186.5). Все внедиагональные корреляции > 0 -- свойство однофакторной модели при beta > 0.

### Adjusted Beta Sigma (задачи 16--18)

Ковариационная матрица на adjusted beta (формула Блюма: beta_adj = 2/3 * beta_hist + 1/3):

```
Sigma_adj = beta_adj * beta_adj' * sigma2_m + diag(sigma2_eps)
```

Adjusted beta сжаты к 1.0, что делает систематический компонент более однородным. Остаточные дисперсии (sigma2_eps) те же -- adjusted beta корректирует только наклон регрессии.

| Режим | Метод | GMVP Return | GMVP Std | Condition # |
|-------|-------|-------------|----------|-------------|
| Unrestricted | Historical Sigma | -9.4% | 13.6% | 186.5 |
| Unrestricted | Market Model Sigma_beta | -8.5% | 13.0% | 158.9 |
| Unrestricted | Adjusted Beta Sigma_adj | -9.1% | 16.6% | 152.4 |
| Long-only | Historical Sigma | 1.2% | 17.4% | -- |
| Long-only | Market Model Sigma_beta | -0.1% | 17.2% | -- |
| Long-only | Adjusted Beta Sigma_adj | -0.3% | 19.9% | -- |

Condition number Sigma_adj = 152.4 -- наименьший среди трёх методов. GMVP std для Sigma_adj выше: сжатие beta к 1.0 убирает "экстремальные" оптимизационные решения, но поднимает нижнюю границу волатильности. Динамика adjusted-beta EF построена для 10 контрольных дат; adjusted beta стабильнее во времени за счёт регрессии к среднему.

### Сравнение трёх подходов к оценке Sigma (задачи 19--20)

Три метода сравнены на выбранном окне (rolling 252d, конец 2025-12-30) и в динамике (10 контрольных дат, 2016--2025).

**Статическое сравнение (задание 19):**

| Свойство | Historical Sigma | Market Model (beta) | Adjusted Beta |
|----------|-----------------|---------------------|---------------|
| Параметров | 465 | 61 | 61 |
| Condition number | 186.5 | 158.9 | 152.4 |
| GMVP std (unrestricted) | 13.6% | 13.0% | 16.6% |
| GMVP std (long-only) | 17.4% | 17.2% | 19.9% |
| GMVP Sharpe (unrestricted) | -1.86 | -1.88 | -1.51 |

Market model -- наименьший GMVP risk (меньше параметров, меньше estimation error). Adjusted beta сжимает бета к 1.0, делая акции более "похожими" -- оптимизатору сложнее найти выигрыш от диверсификации. Все три метода дают отрицательный Sharpe (rf=16%).

**Динамическое сравнение (задание 20*):**

- Наибольшее расхождение между методами -- в кризисные периоды (COVID-2020, февраль 2022)
- Condition number: historical наименее стабилен, adjusted beta наиболее стабилен
- В спокойные периоды (2016--2019) все три метода дают похожие результаты
- Adjusted beta предпочтительнее для стратегического аллокатора (горизонт > 1 года)

### Бонусные задачи со звёздочкой (задача 21)

Все (*)-пункты из заданий 2--20 закрыты:
- 5-дневное скользящее окно с Ledoit-Wolf shrinkage (2607 матриц)
- Обоснование невозможности 1-дневного окна (1 наблюдение)
- Месячный шаг EF dynamics (114 дат)
- Квартальный expanding window (39 дат)
- Полная таблица аудита -- в `data/processed/step9_starred_audit.parquet`

### Two-fund theorem Блэка (задача 22)

Проверена теорема Блэка (1972): любой портфель на unrestricted MVF является линейной комбинацией любых двух других портфелей с этой же границы.

| Проверка | Точность | Вывод |
|----------|----------|-------|
| Численная (GMVP + tangency, 200 портфелей) | max L2 = 1.21e-05 | Теорема подтверждена |
| 5 пар базисных портфелей | max L2 = 1.33e-05 | Работает для любой пары |
| Аналитическая (Merton 1972 closed-form) | ~8.7e-16 | Математически доказано |
| Long-only (constrained) | max L2 = 0.24 | Теорема НЕ выполняется |
| Short <= 25% (constrained) | max L2 = 7.11 | Теорема НЕ выполняется |

Числ. ошибка ~1e-5 обусловлена точностью SLSQP (не нарушением теоремы). Аналитическое решение Merton подтверждает теорему с точностью до machine epsilon. Для constrained случаев binding constraints нарушают линейную структуру MVF.

Практический смысл: в unrestricted режиме достаточно двух "фондов" (GMVP + tangency) для синтеза любого портфеля на frontier. При наличии ограничений на веса это не работает.

---

## Оставшиеся задачи (23--25)

### Задача 23*** -- Monte Carlo frontier

Генерация случайных портфелей (Dirichlet) для визуализации feasible region. Аналитическая EF -- верхняя граница облака. Входные файлы: `selected_mu.parquet`, `selected_cov.parquet`, `ef_long_only.parquet`.

### Задача 24**** -- Maximum risk portfolio

Обратная задача: max w'Sigma*w вместо min. Unrestricted: экстремальные short/long позиции. Long-only: 100% в самую волатильную акцию. Входные файлы: `selected_mu.parquet`, `selected_cov.parquet`.

### Задача 25***** -- Implementation shortfall

Учёт транзакционных издержек в оптимизации. TC = sum |delta_w_i| * c_i, где c_i оценивается из bid-ask spread (OHLCV). Входные файлы: `selected_mu.parquet`, `selected_cov.parquet`, `data/raw/ohlcv_daily.parquet`.

---

## Структура репозитория

```
portfolio_analysis.ipynb          # основной ноутбук (задачи 1--22)
README.md                         # этот файл
requirements.txt                  # зависимости Python
tickers_30.md                     # обоснование выбора 30 акций

scripts/
  01_download_stocks.py           # загрузка OHLCV с MOEX ISS
  02_download_benchmark_and_rates.py  # загрузка IMOEX + ключевая ставка ЦБ
  03_corporate_actions.py         # обработка сплитов и консолидаций
  04_validate_and_finalize.py     # валидация данных
  05_replace_tickers.py           # замена тикеров с неполной историей
  06_replace_lent_with_mvid.py    # замена LENT на MVID
  10_compute_returns.py           # расчёт доходностей из цен
  step3_optimizer.py              # модуль оптимизации по Марковицу
  step4_dynamics.py               # модуль динамики EF

tests/                            # тесты (pytest) по всем задачам
  test_step1_data.py ... test_step10.py

data/
  raw/                            # сырые данные с MOEX и ЦБ
  processed/                      # обработанные данные
  meta/                           # справочники
  export/                         # Excel-экспорт
```

### Файлы данных (data/)

**data/raw/** -- сырые данные:

| Файл | Описание |
|------|----------|
| `ohlcv_daily.parquet` | OHLCV 30 акций, 82670 строк (long format) |
| `benchmark_daily.parquet` | Индекс IMOEX (2015--2025) |
| `risk_free_rate.parquet` | Ключевая ставка ЦБ РФ (дневная) |

**data/meta/** -- справочники:

| Файл | Описание |
|------|----------|
| `instruments.csv` | 30 акций: тикер, компания, сектор |
| `corporate_actions.csv` | Сплиты и консолидации (3 записи) |
| `trading_calendar.parquet` | Торговые дни MOEX |

**data/processed/** -- обработанные данные (задачи 1--22):

| Файл | Задачи | Описание |
|------|--------|----------|
| `prices_adjusted.parquet` | 1 | Цены закрытия, скорректированные на сплиты |
| `returns_daily.parquet` | 2 | Простые дневные доходности (2611 x 30) |
| `rolling_252d_means.parquet` | 2, 9 | Скользящие средние доходности |
| `benchmark_returns.parquet` | 11--16 | Дневные доходности IMOEX |
| `selected_mu.parquet` | 4--22 | Вектор ожидаемых доходностей (30 акций) |
| `selected_cov.parquet` | 4--22 | Историческая ковариационная матрица 30x30 |
| `selected_rf.parquet` | 4--22 | Безрисковая ставка (rf = 16%) |
| `ef_unrestricted.parquet` | 5, 22 | EF без ограничений (200 точек) |
| `ef_unrestricted_weights.pkl` | 5, 22 | Веса 200 портфелей на unrestricted frontier |
| `ef_short_25.parquet` | 6 | EF short <= 25% (200 точек) |
| `ef_long_only.parquet` | 7, 22 | EF long only (200 точек) |
| `ef_long_only_weights.pkl` | 7 | Веса портфелей на long-only frontier |
| `ef_min_2pct.parquet` | 8 | EF min 2% в каждую акцию (200 точек) |
| `ef_portfolios.pkl` | 5--8, 22 | Сводка: GMVP, tangency, EW для 4 режимов |
| `ef_dynamics_rolling_252d.pkl` | 9a | 10 фронтиров, rolling 252d, year-end |
| `ef_dynamics_expanding.pkl` | 9b | 11 фронтиров, расширяющееся окно |
| `ef_dynamics_ewma_094.pkl` | 10 | Динамика EF, EWMA lambda=0.94 |
| `ef_dynamics_ewma_097.pkl` | 10 | Динамика EF, EWMA lambda=0.97 |
| `ef_dynamics_ewma_099.pkl` | 10 | Динамика EF, EWMA lambda=0.99 |
| `beta_historical.parquet` | 12, 13, 16 | Historical beta + sigma_epsilon (OLS, 30 акций) |
| `beta_adjusted.parquet` | 12, 16 | Adjusted beta (Блюм: 2/3*beta + 1/3) |
| `beta_residuals.parquet` | 12, 16 | Остатки OLS-регрессий (252 x 30) |
| `beta_dynamics_rolling_252d.pkl` | 15, 18 | Beta по 10 контрольным датам |
| `ef_beta_unrestricted.parquet` | 14 | EF unrestricted на Sigma_beta |
| `ef_beta_long_only.parquet` | 14 | EF long-only на Sigma_beta |
| `ef_beta_weights.pkl` | 14 | Веса портфелей на beta-based frontier |
| `ef_beta_comparison_table.parquet` | 14 | Таблица: GMVP/tangency для hist vs beta |
| `ef_dynamics_beta_252d.pkl` | 15 | 10 фронтиров на beta, year-end |
| `step10_twofund_verification.parquet` | 22 | Per-portfolio проверка two-fund theorem (200 строк) |
| `step10_twofund_pairs.parquet` | 22 | Ошибки для 5 пар базисных портфелей |
| `step10_twofund_constrained.parquet` | 22 | Ошибки: unrestricted vs constrained |
| `step10_analytical_results.pkl` | 22 | Аналитическая проверка (Merton 1972) |

**data/export/**:

| Файл | Описание |
|------|----------|
| `data_export.xlsx` | Все данные в Excel (32 листа, описание на листе spravka) |

### Скрипты (scripts/)

**Скрипты загрузки данных** -- для воспроизводимости сбора данных с нуля:

| Файл | Описание |
|------|----------|
| `01_download_stocks.py` | Скачивает OHLCV 30 акций с MOEX ISS API |
| `02_download_benchmark_and_rates.py` | Скачивает IMOEX и ключевую ставку ЦБ |
| `03_corporate_actions.py` | Обработка сплитов (GMKN, PLZL) и консолидации (VTBR) |
| `04_validate_and_finalize.py` | Валидация данных: пропуски, аномалии, полнота |
| `05_replace_tickers.py` | Замена тикеров с неполной историей |
| `06_replace_lent_with_mvid.py` | Финальная замена LENT на MVID |
| `10_compute_returns.py` | Расчёт простых и лог-доходностей из цен |

**Модули-утилиты** -- функции, используемые в ноутбуке и других скриптах:

| Модуль | Ключевые функции |
|--------|------------------|
| `step3_optimizer.py` | `find_gmvp()`, `find_tangency()`, `build_efficient_frontier()`, `portfolio_return()`, `portfolio_volatility()`, `portfolio_sharpe()` |
| `step4_dynamics.py` | `subsample_dates()`, `build_frontier_series()`, `load_historical_rf()`, `get_rf_for_date()`, `plot_frontier_dynamics()` |

Все ключевые функции также определены inline в ноутбуке, так что для работы в Colab внешние модули не обязательны.

### Тесты (tests/)

17 файлов с тестами (pytest), проверяют корректность данных и расчётов по всем задачам:

| Файл | Задачи | Тестов |
|------|--------|--------|
| `test_step1_data.py` | 1 | ~30 |
| `test_step2_returns.py` | 2 | ~15 |
| `test_step2_data.py` | 2--3 | ~35 |
| `test_step3_stage1.py` | 4 | ~25 |
| `test_step3.py` | 5--8 | ~65 |
| `test_step4_dynamics.py` | 9--10 | ~35 |
| `test_step4.py` | 9--10 | ~25 |
| `test_step5.py` | 11--12 | ~30 |
| `test_step6_phase_a.py` | 13 | ~20 |
| `test_step6_phase_b.py` | 14 | ~35 |
| `test_step6_phase_c.py` | 15 | ~20 |
| `test_step7_phase_a.py` | 16 | ~20 |
| `test_step7_phase_b.py` | 17 | ~35 |
| `test_step7_phase_c.py` | 18 | ~25 |
| `test_step8.py` | 19--20 | ~60 |
| `test_step9.py` | 21 | ~90 |
| `test_step10.py` | 22 | ~64 |

### Визуализации (temp/)

Графики для презентации и анализа. 43 файла, разбиты на слайдовые и запасные (для вопросов на защите). Полный список -- в `presentation.md`.

### Excel-файл (data_export.xlsx)

32 листа:

| Лист | Описание |
|------|----------|
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
| beta_statistics | OLS-диагностика (18 показателей) |
| beta_cov_matrix | Sigma_beta (market model, 30x30) |
| ef_beta_comparison | Сравнение historical vs beta EF |
| adj_beta_cov_matrix | Sigma_adj (adjusted beta, 30x30) |
| ef_adj_beta_comparison | Сравнение historical vs adj_beta EF |
| ef_adj_beta_key_portfolios | GMVP/tangency для 3 методов |
| comparison_static | Статическое сравнение 3 подходов |
| comparison_dynamics | Динамическое сравнение (10 дат x 3 метода) |
| weights_analysis | Анализ весов портфелей |
| risk_decomposition | Систематический vs идиосинкратический риск |
| starred_audit | Аудит (*)-пунктов из заданий 2--20 |
| methods_overview | Сводка по методам оценки ковариации |
| ef_monthly_gmvp | Траектория GMVP (114 месячных дат) |
| twofund_verification | Per-portfolio проверка two-fund theorem |
| twofund_pairs | Ошибки для 5 пар базисных портфелей |
| twofund_constrained | Сравнение ошибок: unrestricted vs constrained |

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

Полный список с версиями -- в `requirements.txt`.

---

## Особенности данных

1. **Период 02--03.2022 (приостановка торгов на MOEX):** торги были остановлены на несколько недель. Пропуск остаётся в данных, описан в методологии.

2. **CBOM (МКБ):** торги начались 02.07.2015, поэтому общий период всех 30 акций начинается с этой даты (а не с 01.01.2015).

3. **Отрицательный Sharpe ratio:** при rf = 16% доходность GMVP ниже безрисковой ставки во всех режимах. Это не ошибка -- при ключевой ставке 16% акции объективно проигрывают депозиту на годовом горизонте.

4. **Short_25 frontier:** при визуализации уходит далеко вправо (std до 230%), при построении графиков нужно обрезать ось X.
