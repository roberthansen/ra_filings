import os

kw_user = {
    'uid' : os.getenv('KITEWORKS_UID'),
    'passwd' : os.getenv('KITEWORKS_PASSWD'),
}
kw_api_client = {
    'app_id' : os.getenv('KITEWORKS_API_CLIENT_APP_ID'),
    'app_key' : os.getenv('KITEWORKS_API_CLIENT_APP_KEY'),
    'signature' : os.getenv('KITEWORKS_API_SIGNATURE'),
}
ezdb_user = {
    'uid' : os.getenv('EZDB_UID'),
    'passwd' : os.getenv('EZDB_PASSWD'),
}