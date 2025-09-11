# ============================
# 0) Libraries
# ============================
install.packages(c("RPostgres","dplyr","tidyr","zoo","readxl","stringr","ggplot2"))
library(RPostgres)
library(dplyr)
library(tidyr)
library(zoo)
library(readxl)
library(stringr)
library(ggplot2)

install.packages("tidyverse") 
library(tidyverse)        

# ============================
# 1) Connect to WRDS + Pull Compustat
# ============================
wrds <- dbConnect(
  Postgres(),
  host   = "wrds-pgdata.wharton.upenn.edu",
  port   = 9737,
  dbname = "wrds",
  sslmode= "require",
  user   = "harrisonvu"
)

qry <- "
SELECT gvkey, conm, fyear, naicsh, sich, 
       at, che, lt, capx, xrd, sale,
       oancf, ib, cogs, invt, dltt, dlc,
       oibdp, xint, txt, dvc
FROM comp.funda
WHERE indfmt = 'INDL'
  AND datafmt = 'STD'
  AND popsrc = 'D'
  AND consol = 'C'
  AND fyear BETWEEN 2010 AND 2024
"

res   <- dbSendQuery(wrds, qry)
funda <- dbFetch(res, n = -1)
dbClearResult(res)

funda <- funda %>%
  mutate(sich = as.numeric(sich)) %>%
  filter(!(sich >= 4900 & sich <= 4999),   # drop utilities
         !(sich >= 6000 & sich <= 6999))   # drop financials


# ============================
# 2) Clean panel + core variables
# ============================

funda <- funda %>%
  # Convert to numeric
  mutate(across(c(at, che, lt, capx, xrd, sale, oancf, ib, cogs, invt,
                  dltt, dlc, oibdp, xint, txt, dvc), as.numeric)) %>%
  # Remove rows with missing critical inputs (asset, cash, sales, debt, etc.)
  filter(!if_any(c(at, che, sale, dltt, dlc, oibdp, xint, txt, dvc), is.na)) %>%
  filter(at > 0) %>%
  mutate(naics6 = suppressWarnings(as.integer(naicsh))) %>%
  arrange(gvkey, fyear) %>%
  group_by(gvkey) %>%
  arrange(fyear, .by_group = TRUE) %>%
  mutate(
    # Dependent variable
    # Adjusted total assets (exclude cash and marketable securities)
    adj_assets = at - che,
    adj_assets = ifelse(adj_assets > 0, adj_assets, NA_real_),
    
    # Cash measures
    cash_holdings = ifelse(!is.na(adj_assets), che / adj_assets, NA_real_),
  
    # Size and growth
    size_ln_at   = ifelse(adj_assets > 0, log(adj_assets), NA_real_),
    sales_growth = ifelse(lag(sale) > 0, (sale - lag(sale)) / lag(sale), NA_real_),
    
    # Cash flow / assets
    cash_flow = ifelse(!is.na(adj_assets), (oibdp - xint - txt - dvc) / adj_assets, NA_real_),
    
    # Rolling 5-year cash flow volatility
    cf_vol_5y = zoo::rollapplyr(cash_flow, width = 5, FUN = sd, partial = FALSE, fill = NA_real_),
    
    # Leverage
    debt          = dltt + dlc,
    leverage_debt = ifelse(!is.na(adj_assets), debt / adj_assets, NA_real_),
    
    # Investment & innovation
    capex_over_at = ifelse(!is.na(adj_assets), capx / adj_assets, NA_real_),
    rd_over_sales = ifelse(sale > 0, xrd / sale, NA_real_)
  ) %>%
  ungroup()


# Optional trimming for extreme outliers
funda <- funda %>%
  mutate(
    cash_holdings       = ifelse(is.finite(cash_holdings) & cash_holdings >= 0 & cash_holdings <= 1, cash_holdings, NA_real_),
    leverage_debt       = ifelse(is.finite(leverage_debt) & leverage_debt >= 0, leverage_debt, NA_real_),
    capex_over_at       = ifelse(is.finite(capex_over_at) & capex_over_at >= 0, capex_over_at, NA_real_),
    rd_over_sales       = ifelse(is.finite(rd_over_sales) & rd_over_sales >= 0, rd_over_sales, NA_real_)
  )

#Winsorize to reduce influence of extreme but valid outliers

install.packages("DescTools")
library(DescTools)

funda <- funda %>%
  mutate(
    cash_holdings      = Winsorize(cash_holdings, probs = c(0.01, 0.99), na.rm = TRUE),
    leverage_debt      = Winsorize(leverage_debt, probs = c(0.01, 0.99), na.rm = TRUE),
    capex_over_at      = Winsorize(capex_over_at, probs = c(0.01, 0.99), na.rm = TRUE),
    rd_over_sales      = Winsorize(rd_over_sales, probs = c(0.01, 0.99), na.rm = TRUE),
    cash_flow          = Winsorize(cash_flow, probs = c(0.01, 0.99), na.rm = TRUE),
    sales_growth       = Winsorize(sales_growth, probs = c(0.01, 0.99), na.rm = TRUE),
    cf_vol_5y          = Winsorize(cf_vol_5y, probs = c(0.01, 0.99), na.rm = TRUE)
  )

# ============================
# 3) Ingest WUI (USA only) from WUI Excel (sheet = "T2")
#     - Keep USA only
#     - Annualize by averaging quarters within year
# ============================
# Put the WUI excel file in your working directory and update the filename if needed
wui_t2 <- read_excel("WUI_Data.xlsx", sheet = "T2")

wui_long <- wui_t2 %>%
  filter(!is.na(year), str_detect(year, "^\\d{4}q[1-4]$")) %>%
  mutate(fyear = as.integer(str_sub(year, 1, 4))) %>%
  pivot_longer(cols = -c(year, fyear), names_to = "iso3", values_to = "wui")

wui_annual <- wui_long %>%
  filter(iso3 == "USA") %>%
  group_by(fyear) %>%
  summarise(wui_usa = mean(wui, na.rm = TRUE), .groups = "drop")

# Remove any previous WUI columns that may exist, then merge clean USA WUI
funda <- funda %>%
  select(-matches("(?i)^wui_")) %>%
  left_join(wui_annual, by = "fyear")

# ============================
# 4) Descriptive statistics table (example)
# ============================
stat_row <- function(x, var_label, N_total = NA_integer_) {
  tibble(
    Variable          = var_label,
    Mean              = mean(x, na.rm = TRUE),
    `25th Percentile` = quantile(x, 0.25, na.rm = TRUE),
    Median            = median(x, na.rm = TRUE),
    `75th Percentile` = quantile(x, 0.75, na.rm = TRUE),
    `Std. Dev.`       = sd(x, na.rm = TRUE),
    N                 = sum(!is.na(x))
  )
}



#---- Build the table row-by-row for available variables ----#
desc_tbl <- bind_rows(
  stat_row(funda$cash_holdings,        "Cash/assets"),
  stat_row(funda$size_ln_at,           "Firm size (ln assets)"),
  stat_row(funda$rd_over_sales,        "R&D/sales"),
  stat_row(funda$cash_flow,            "Cash flow/assets"),
  stat_row(funda$capex_over_at,        "Capital expenditures/assets"),
  stat_row(funda$leverage_debt,        "Total leverage (debt/assets)"),
  stat_row(funda$sales_growth,         "Sales growth"),
  stat_row(funda$cf_vol_5y,            "Cash flow volatility (5y, within-firm)"),
  # If you merged WUI USA as `wui_usa`:
  if ("wui_usa" %in% names(funda)) stat_row(funda$wui_usa, "WUI (USA, annual avg)") else NULL
) %>%
  # pretty rounding
  mutate(across(c(Mean, `25th Percentile`, Median, `75th Percentile`, `Std. Dev.`), ~round(.x, 3)))

desc_tbl

# ============================
# 5) Simple visuals 
# ============================

# 5a) Time trend of average cash holdings vs WUI (by fiscal year)
cash_year <- funda %>%
  group_by(fyear) %>%
  summarise(
    avg_cash = mean(cash_holdings, na.rm = TRUE),
    wui_usa  = mean(wui_usa, na.rm = TRUE)
  ) %>%
  filter(!is.na(fyear))

# Plot cash trend
ggplot(cash_year, aes(fyear, avg_cash)) +
  geom_line() +
  geom_point() +
  labs(title = "Average Cash-to-Assets by Fiscal Year",
       x = "Fiscal Year", y = "Average Cash/Assets")

# Plot WUI trend
ggplot(cash_year, aes(fyear, wui_usa)) +
  geom_line() +
  geom_point() +
  labs(title = "World Uncertainty Index (USA) by Fiscal Year",
       x = "Fiscal Year", y = "WUI (USA)")

# 5b) Scatter: firm-level cash vs WUI (yearly match)
#     (This is noisy but gives intuition for H1)
ggplot(funda %>% filter(!is.na(cash_holdings), !is.na(wui_usa)),
       aes(wui_usa, cash_holdings)) +
  geom_point(alpha = 0.2) +
  geom_smooth(method = "lm", se = TRUE) +
  labs(title = "Cash Holdings vs WUI (USA) – Firm-Year",
       x = "WUI (USA)", y = "Cash/Assets")

# ============================
# 6) Notes for Assessment 2 write-up
# ============================
# - You now have: cash_holdings (DV), controls (size, leverage, CF, CAPEX, R&D, CF volatility),
#   and wui_usa (uncertainty). This is enough to test H1 descriptively and with a baseline regression.
# - Later, you can add:
#     * Tariff cost exposure (industry-year tariffs merged by NAICS)
#     * Export sales ratio (industry export share proxy)
# - Keep the code modular so you can drop in new merges without changing the core pipeline.



cash_year_std <- cash_year %>%
  mutate(
    z_cash = scale(avg_cash)[,1],
    z_wui  = scale(wui_usa)[,1]
  ) %>%
  pivot_longer(c(z_cash, z_wui), names_to = "series", values_to = "z")

ggplot(cash_year_std, aes(fyear, z, color = series)) +
  geom_line() + geom_point() +
  labs(title = "Cash vs Uncertainty (Standardized)",
       x = "Fiscal Year", y = "Z-score") +
  scale_color_discrete(labels = c("Cash/Assets", "WUI (USA)"))


ggplot(funda %>% filter(!is.na(cash_holdings), !is.na(wui_usa)),
       aes(wui_usa, cash_holdings)) +
  geom_point(alpha = 0.15) +
  geom_smooth(method = "lm", se = TRUE) +
  labs(title = "Firm-Year Cash Holdings vs WUI (USA)",
       x = "WUI (USA)", y = "Cash/Assets")


