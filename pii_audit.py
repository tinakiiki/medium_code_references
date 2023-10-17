table_list = []

# list all directories/files in bucket
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token =AWS_SESSION_TOKEN
)

#Then use the session to get the resource
s3 = session.resource('s3')

my_bucket = s3.Bucket('BUCKET_NAME')
# print list of files
for my_bucket_object in my_bucket.objects.all():
    table_list.append(my_bucket_object.key)
pii_col_dict = {}
pii_search_words = ['address','name','email']

#### check each table for PII key words
client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token =AWS_SESSION_TOKEN)

for file in table_list:
    pii_column_name_list = []
    response = client.get_object(
        Bucket=BUCKET_NAME,
        Key=file,
    )

    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        df = pd.read_csv(response.get("Body"))
        mask = df.apply(lambda col: col.astype(str).str.contains('|'.join(pii_search_words)).any(),
                axis=0)
        # Select columns which contains a sub-string 'X'
        row_check_df = df.loc[: , mask]
        df_pii_rows = [col for col in row_check_df.columns]
        col_name_check= df.loc[:,df.columns.str.contains('|'.join(pii_search_words))]
        df_pii_cols = [col for col in col_name_check.columns]
        if df_pii_rows != '[]' or df_pii_cols != '[]':
            pii_col_dict[file] = ",".join(str(e) for e in df_pii_rows+df_pii_cols)

res_df = df = pd.DataFrame(pii_col_dict.items(), columns=['table_name', 'columns_to_check']).reset_index(drop=True)
res_df = res_df[df["columns_to_check"] != ""]
res_df.to_csv("pii_results.csv") 
