# GCP credentials for BigQuery (Copilot, scripts)

The backend uses **Application Default Credentials** for BigQuery. You can use either method below.

---

## Option A: gcloud (no key file) — recommended for local dev

1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) if needed.
2. From a terminal run:
   ```powershell
   gcloud auth application-default login
   ```
   Sign in in the browser with the account that has access to your GCP project.
3. (Optional) If you use a specific project for BigQuery:
   ```powershell
   gcloud auth application-default set-quota-project YOUR_BQ_PROJECT_ID
   ```
4. Leave `GOOGLE_APPLICATION_CREDENTIALS` **unset** in `.env`. BigQuery will use these credentials.

You can also run the helper script from the repo root:
```powershell
.\backend\scripts\gcloud_login.ps1
```

---

## Option B: Service account key file

1. In [Google Cloud Console](https://console.cloud.google.com/) → **IAM & Admin** → **Service Accounts** → create or select one → **Keys** → **Add key** → **Create new key** → **JSON** → download.
2. Save the JSON file here as `gcp-key.json` (or another name).
3. In your `.env` (repo root), set:
   ```env
   GOOGLE_APPLICATION_CREDENTIALS=backend/secrets/gcp-key.json
   ```
   Use an absolute path if the app is started from a different working directory.
4. Restart the backend.

**Never commit** key files. `*.json` in this folder is ignored by git.
