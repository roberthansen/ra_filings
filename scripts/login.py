import os

kw_user = {
    'uid' : os.getenv('KITEWORKS_UID_SECRET'),
    'passwd' : os.getenv('KITEWORKS_PASSWD_SECRET'),
}
kw_api_client = {
    'app_id' : os.getenv('KITEWORKS_API_CLIENT_APP_ID_SECRET'),
    'app_key' : os.getenv('KITEWORKS_API_CLIENT_APP_KEY_SECRET'),
    'signature' : os.getenv('KITEWORKS_API_SIGNATURE_SECRET'),
}
ezdb_user = {
    'uid' : os.getenv('EZDB_UID_SECRET'),
    'passwd' : os.getenv('EZDB_PASSWD_SECRET'),
}