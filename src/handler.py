import json
import awswrangler as wr

def lambda_handler(event, context):
    # TODO implement
    print (event)

    if "Records" not in event:
        return {
            "statusCode": 400
        }

    if "body" not in event:
        return {"statusCode": 400}
    else :
        body_event = json.loads(event["body"])
        print (body_event)
        name_bucket = body_event["detail"]["bucket"]["name"]
        file_size = body_event["detail"]["object"]["size"]
        time_event = body_event["time"]
        file_name = body_event['detail']['object']['key']

        #lire le csv depuis le S3
        s3_path = s3_path = f"s3://{name_bucket}/{file_name}"
        df = wr.s3.read_csv(s3_path)

        # Ajouter les métadonnées à chaque ligne
        df["bucket"] = name_bucket
        df["file_name"] = file_name
        df["file_size"] = file_size
        df["file_time_send"] = time_event

        #ecriture dans DynamoDB
        wr.dynamodb.put_df(
            table_name="ConsumptionDataIngestion",
            df=df
        )


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }