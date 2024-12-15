CREATE TABLE customers( 
  Number UInt32 
  ,Age UInt8 
  ,Income  Float64
  ,Spending_Score       Float64
  ,Membership_Years UInt8
  ,Purchase_Frequency Float64
  ,Last_Purchase_Amount Float64
)ENGINE = MergeTree() ORDER BY Number;
