import json
import awswrangler as wr

def lambda_handler(event, context):
    # TODO implement
    print (event)

    if "Records" not in event:
        return {
            "statusCode": 400
        }
    print("Records existe")

    for record in event['Records']:
        if "body" not in record:
            return {"statusCode": 400}
        else :
            print("BODY existe")
            body_event = json.loads(event["body"])
            print (body_event)
            name_bucket = body_event["detail"]["bucket"]["name"]
            file_size = body_event["detail"]["object"]["size"]
            time_event = body_event["time"]
            file_name = body_event['detail']['object']['key']

            #lire le csv depuis le S3
            s3_path = f"s3://{name_bucket}/{file_name}"
            df = wr.s3.read_csv(s3_path)
            print("avant l'ajout des métadonnées")
            # Ajouter les métadonnées à chaque ligne
            df["bucket"] = name_bucket
            df["file_name"] = file_name
            df["file_size"] = file_size
            df["file_time_send"] = time_event
            print(name_bucket, file_name, file_size)
            print("avant écriture dans la table")
            #ecriture dans DynamoDB
            wr.dynamodb.put_df(
                table_name="consumption-data-ingestion-dybnamodb-dev",
                df=df
            )
            print("message ecrit")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }