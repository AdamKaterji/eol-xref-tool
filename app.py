import json
import logging
import os
import threading
import uuid
from datetime import date, datetime
from pathlib import Path
from queue import Queue, Empty

from flask import Flask, Response, jsonify, render_template, request, send_file

from models import CrossRefResult
from scrapers import scrape_suppliers, SUPPLIER_MAP
from session import get_session
from ti_xref import run_crossref_batch
import excel_report

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

OUTPUT_DIR = Path("/tmp/eol_reports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

JOBS: dict = {}


@app.route("/")
def index():
    suppliers = list(SUPPLIER_MAP.keys())
    return render_template("index.html", suppliers=suppliers)


@app.route("/run", methods=["POST"])
def run_job():
    data      = request.get_json()
    suppliers = data.get("suppliers", [])
    from_date = data.get("from_date", "")
    to_date   = data.get("to_date", str(date.today()))

    if not suppliers:
        return jsonify({"error": "Select at least one supplier"}), 400
    if not from_date:
        return jsonify({"error": "Start date is required"}), 400
    try:
        since = datetime.strptime(from_date, "%Y-%m-%d").date()
    except ValueError:
        return jsonify({"error": "Invalid date — use YYYY-MM-DD"}), 400
    if since > date.today():
        return jsonify({"error": "Start date cannot be in the future"}), 400

    job_id = str(uuid.uuid4())[:8]
    q: Queue = Queue()
    JOBS[job_id] = {
        "status": "running", "queue": q,
        "results": [], "report_path": None, "summary": {},
        "since": str(since), "to": to_date, "suppliers": suppliers,
    }
    threading.Thread(target=_run_job, args=(job_id, suppliers, since), daemon=True).start()
    return jsonify({"job_id": job_id})


@app.route("/stream/<job_id>")
def stream(job_id):
    if job_id not in JOBS:
        return Response("Job not found", status=404)

    def generate():
        q = JOBS[job_id]["queue"]
        while True:
            try:
                msg = q.get(timeout=30)
                yield f"data: {json.dumps(msg)}\n\n"
                if msg.get("type") in ("done", "error"):
                    break
            except Empty:
                yield 'data: {"type":"ping"}\n\n'

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/status/<job_id>")
def status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({"status": job["status"], "summary": job["summary"],
                    "report_ready": job["report_path"] is not None})


@app.route("/download/<job_id>")
def download(job_id):
    job = JOBS.get(job_id)
    if not job or not job["report_path"]:
        return jsonify({"error": "Report not ready"}), 404
    return send_file(job["report_path"], as_attachment=True,
                     download_name=Path(job["report_path"]).name)


def _emit(job_id):
    def fn(supplier, message):
        JOBS[job_id]["queue"].put({
            "type": "progress", "supplier": supplier,
            "message": message, "ts": datetime.now().strftime("%H:%M:%S"),
        })
    return fn


def _run_job(job_id: str, suppliers: list, since: date):
    job  = JOBS[job_id]
    emit = _emit(job_id)
    q    = job["queue"]
    try:
        q.put({"type": "stage", "message": "Scraping supplier EOL pages..."})
        parts = scrape_suppliers(suppliers, since, emit=emit)

        if not parts:
            q.put({"type": "done",
                   "message": "No EOL parts found for the selected date range and suppliers."})
            job["status"]  = "done"
            job["summary"] = {"total": 0, "matched": 0, "no_match": 0,
                              "drop_in": 0, "p2p": 0, "similar": 0, "rate": "0%"}
            return

        q.put({"type": "stage",
               "message": f"Found {len(parts)} EOL parts. Running TI cross-reference..."})

        session = get_session()
        results = run_crossref_batch(parts, session, emit=emit)
        job["results"] = results

        q.put({"type": "stage", "message": "Building Excel report..."})
        fname    = f"EOL_TI_Xref_{since}_to_{date.today()}.xlsx"
        out_path = str(OUTPUT_DIR / fname)
        excel_report.generate(results, out_path)
        job["report_path"] = out_path

        matched = sum(1 for r in results if r.ti_alternatives)
        drop_in = sum(1 for r in results if r.ti_alternatives and
                      r.ti_alternatives[0].match_type == "Drop-in Replacement")
        p2p     = sum(1 for r in results if r.ti_alternatives and
                      r.ti_alternatives[0].match_type == "Pin-to-Pin")
        similar = sum(1 for r in results if r.ti_alternatives and
                      r.ti_alternatives[0].match_type in ("Similar", "Same Functionality"))

        summary = {"total": len(results), "matched": matched,
                   "no_match": len(results) - matched,
                   "drop_in": drop_in, "p2p": p2p, "similar": similar,
                   "rate": f"{matched/len(results)*100:.0f}%" if results else "0%"}
        job["summary"] = summary
        job["status"]  = "done"
        q.put({"type": "done", "summary": summary,
               "message": f"Done — {matched}/{len(results)} parts matched to TI alternatives."})

    except Exception as e:
        log.exception(f"Job {job_id} failed")
        job["status"] = "error"
        q.put({"type": "error", "message": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
