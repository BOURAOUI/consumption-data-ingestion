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
            body_event = json.loads(record["body"])
            print (body_event)
            name_bucket = body_event["detail"]["bucket"]["name"]
            file_name = body_event['detail']['object']['key']
            print("métadonnées extraite")
            #lire le csv depuis le S3
            try:
                s3_path = f"s3://{name_bucket}/{file_name}"
                print(f"Tentative de lecture du fichier : {s3_path}")
                df = wr.s3.read_csv(s3_path)
            except Exception as e:
                print(f"Erreur lors de la lecture du fichier S3 : {e}")
                return {
                    "statusCode": 500,
                    "body": json.dumps("Erreur de lecture du fichier S3")
                }

            print("avant écriture dans la table")
            #ecriture dans DynamoDB (je convertit tous en str)
            df = df.astype(str)
            wr.dynamodb.put_df(
                table_name="consumption-data-ingestion-dybnamodb-dev",
                df=df
            )
            print("message ecrit")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }