CREATE TABLE customers_transformed( 
            Age Int32,
            AverageIncome Float64,
            UpdateVersion UInt64
) ENGINE = ReplacingMergeTree(UpdateVersion)
ORDER BY Age



