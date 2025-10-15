### calendar.csv
| Column | Type | Non-Null Count | Example |
|---------|------|----------------|----------|
| date | string | 500 | 2011-01-29 |
| wm_yr_wk | int64 | 500 | 11101 |
| weekday | string | 500 | Saturday |
| wday | int64 | 500 | 1 |
| month | int64 | 500 | 1 |
| year | int64 | 500 | 2011 |
| d | string | 500 | d_1 |
| event_name_1 | string | 42 | SuperBowl |
| event_type_1 | string | 42 | Sporting |
| event_name_2 | string | 1 | Easter |
| event_type_2 | string | 1 | Cultural |
| snap_CA | int64 | 500 | 0 |
| snap_TX | int64 | 500 | 0 |
| snap_WI | int64 | 500 | 0 |

### sales_train_evaluation.csv
| Column | Type | Non-Null Count | Example |
|---------|------|----------------|----------|
| id | string | 500 | HOBBIES_1_001_CA_1_evaluation |
| item_id | string | 500 | HOBBIES_1_001 |
| dept_id | string | 500 | HOBBIES_1 |
| cat_id | string | 500 | HOBBIES |
| store_id | string | 500 | CA_1 |
| state_id | string | 500 | CA |
| d_1 | int64 | 500 | 0 |
...
| d_1941 | int64 | 500 | 1 |

### sales_train_validation.csv
| Column | Type | Non-Null Count | Example |
|---------|------|----------------|----------|
| id | string | 500 | HOBBIES_1_001_CA_1_validation |
| item_id | string | 500 | HOBBIES_1_001 |
| dept_id | string | 500 | HOBBIES_1 |
| cat_id | string | 500 | HOBBIES |
| store_id | string | 500 | CA_1 |
| state_id | string | 500 | CA |
| d_1 | int64 | 500 | 0 |
...
| d_1913 | int64 | 500 | 1 |

### sample_submission.csv
| Column | Type | Non-Null Count | Example |
|---------|------|----------------|----------|
| id | string | 500 | HOBBIES_1_001_CA_1_validation |
| F1 | int64 | 500 | 0 |
...
| F28 | int64 | 500 | 0 |

### sell_prices.csv
| Column | Type | Non-Null Count | Example |
|---------|------|----------------|----------|
| store_id | string | 500 | CA_1 |
| item_id | string | 500 | HOBBIES_1_001 |
| wm_yr_wk | int64 | 500 | 11325 |
| sell_price | float64 | 500 | 9.58 |

