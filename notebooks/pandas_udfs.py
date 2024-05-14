import numpy as np
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("float")
def teehr_kling_gupta_efficiency(p: pd.Series, s: pd.Series) -> float:
    if len(s) == 0 or len(s) == 0:
        return np.nan
    std_p = np.std(p)
    mean_p = np.mean(p)
    std_s = np.std(s)

    if std_p == 0 or mean_p == 0 or std_s == 0:
        return np.nan
        
    # Pearson correlation coefficient
    linear_correlation = np.corrcoef(s, p)[0,1]

    # Relative variability
    relative_variability = std_s / std_p

    # Relative mean
    relative_mean = np.mean(s) / mean_p

    # Scaled Euclidean distance
    euclidean_distance = np.sqrt(
        (1 * (linear_correlation - 1.0)) ** 2.0 + 
        (1 * (relative_variability - 1.0)) ** 2.0 + 
        (1* (relative_mean - 1.0)) ** 2.0
        )

    # Return KGE
    return 1.0 - euclidean_distance

# spark.udf.register("teehr_kling_gupta_efficiency", teehr_kling_gupta_efficiency)

@pandas_udf("float")
def teehr_root_mean_squared_error(p: pd.Series, s: pd.Series) -> float:
    if len(s) == 0 or len(s) == 0:
        return np.nan
    return np.sqrt(np.mean((s-p)**2))
    
# spark.udf.register("teehr_root_mean_squared_error", teehr_root_mean_squared_error)

@pandas_udf("float")
def teehr_relative_bias(p: pd.Series, s: pd.Series) -> float:
    if len(s) == 0 or len(s) == 0:
        return np.nan

    sum_p = np.sum(p)
    if sum_p == 0:
        return np.nan
        
    diff = s - p
    return np.sum(diff)/sum_p
    
# spark.udf.register("teehr_relative_bias", teehr_relative_bias)

@pandas_udf("float")
def teehr_r_squared(p: pd.Series, s: pd.Series) -> float:
    if len(s) == 0 or len(s) == 0:
        return np.nan

    std_p = np.std(p)
    std_s = np.std(s)

    if std_p == 0 or std_s == 0:
        return np.nan
        
    # Pearson correlation coefficient
    pearson_correlation = (np.corrcoef(s, p))[0][1]
    
    return (np.power(pearson_correlation, 2))
    
# spark.udf.register("teehr_r_squared", teehr_r_squared)