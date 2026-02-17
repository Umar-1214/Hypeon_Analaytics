# When to run backend and frontend

## One-time setup (already done if you ran it once)

From the **hypeon** folder:

1. **Start Postgres**
   ```bash
   docker compose -f infra/compose/docker-compose.yml up -d postgres
   ```

2. **Run migrations**
   ```powershell
   $env:DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/hypeon"
   $env:PYTHONPATH = "."
   python -m alembic -c infra/migrations/alembic.ini upgrade head
   ```

3. **Generate sample data** (optional; for dashboard with data)
   ```bash
   python scripts/generate_sample_data.py
   ```

---

## Every time you want to use the app

### 1. Run the backend first

In a terminal, from the **hypeon** folder:

```powershell
cd c:\Users\umarf\Desktop\Code_base\Hypeon_Analaytics\hypeon
$env:DATABASE_URL = "postgresql://postgres:postgres@localhost:5433/hypeon"
$env:PYTHONPATH = "."
python -m uvicorn apps.api.src.app:app --reload --host 0.0.0.0 --port 8000
```

Leave this running. You should see: `Uvicorn running on http://0.0.0.0:8000`.

- API: http://localhost:8000  
- API docs: http://localhost:8000/docs  

---

### 2. Then run the frontend

In a **second** terminal, from the **hypeon/apps/web** folder:

```bash
cd c:\Users\umarf\Desktop\Code_base\Hypeon_Analaytics\hypeon\apps\web
npm install
npm run dev
```

Leave this running. Open: **http://localhost:5173**

The dashboard will proxy API calls to the backend on port 8000. Use **Run pipeline** on the dashboard, then pick dates 2025-01-01 to 2025-03-31 to see metrics.

---

**Order:** Backend first → then frontend. Stop with Ctrl+C in each terminal when done.
