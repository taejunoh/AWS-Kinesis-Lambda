import json
import boto3
import yfinance as yf

tickers = ['FB', 'SHOP', 'BYND', 'NFLX', 'PINS', 'SQ', 'TTD', 'OKTA', 'SNAP', 'DDOG']
start = "2020-12-01"
end = "2020-12-02"

def lambda_handler(event, context):
  
  kinesis = boto3.client('kinesis', "us-east-2")
  
  for ticker in tickers:
    
    data = yf.download(ticker, start=start, end=end, interval = "5m")
  
    for datetime, row in data.iterrows():
      output = {'name' : ticker}
      output['high'] = round(row['High'], 2)
      output['low'] = round(row['Low'], 2)
      output['ts'] = str(datetime)
      to_json = json.dumps(output)+"\n"
      kinesis.put_record(
                StreamName="yfinance_stream",
                Data=to_json,
                PartitionKey="partitionkey"
                )
