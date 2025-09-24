from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID = "989685468950-0nt9j1bujqjfitljp0kbfktbbfll9rh1.apps.googleusercontent.com"          # el de tu OAuth Client (Web o Desktop)
CLIENT_SECRET = "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"  # su secreto
SCOPES = ["https://www.googleapis.com/auth/drive"]  # <-- scope COMPLETO

flow = InstalledAppFlow.from_client_config(
    {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    },
    scopes=SCOPES,
)

creds = flow.run_local_server(port=0)
print("REFRESH_TOKEN:", creds.refresh_token)
