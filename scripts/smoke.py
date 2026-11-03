"""HTTP acceptance for a RUNNING mock gateway. Python standard library only."""
import argparse
import json
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--token", required=True)
    parser.add_argument("--output", default="acceptance-output/smoke.json")
    args = parser.parse_args()

    def call(method, path, body=None, content_type="application/json", expected=200):
        payload = body if isinstance(body, bytes) else (json.dumps(body).encode() if body is not None else None)
        request = urllib.request.Request(args.base + path, data=payload, method=method,
            headers={"Authorization": "Bearer " + args.token, "Content-Type": content_type})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                assert response.status == expected, (path, response.status)
                data = response.read()
                return json.loads(data) if response.headers.get_content_type() == "application/json" else data
        except urllib.error.HTTPError as error:
            raise AssertionError((path, error.code, error.read().decode())) from error

    ready = call("GET", "/readyz")
    assert ready["mode"] == "mock", "This smoke suite uses simulated FASTQ; use it only with mock_slurm=true"
    templates = call("GET", "/api/v1/workflows")
    assert len(templates) >= 2
    records = []
    for template in templates[:2]:
        inputs = {}
        for name in template["inputs"]:
            boundary = "hpc" + uuid.uuid4().hex
            body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{name}.fastq\"\r\n"
                "Content-Type: application/octet-stream\r\n\r\n@demo\nACGT\n+\nIIII\n"
                f"\r\n--{boundary}--\r\n").encode()
            uploaded = call("POST", "/api/v1/files", body, "multipart/form-data; boundary=" + boundary, 201)
            inputs[name] = uploaded["id"]
            assert call("GET", f"/api/v1/files/{uploaded['id']}/content")["content"].startswith("@demo")
        run = call("POST", f"/api/v1/workflows/{template['id']}/deploy", {"inputs": inputs, "parameters": {"threads": 2}}, expected=201)
        assert run["state"] == "DEPLOYED"
        run = call("POST", f"/api/v1/runs/{run['id']}/execute", expected=202)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            run = call("GET", f"/api/v1/runs/{run['id']}")
            if run["state"] in ("COMPLETED", "FAILED", "CANCELLED"):
                break
            time.sleep(0.5)
        assert run["state"] == "COMPLETED", run
        report = call("POST", f"/api/v1/runs/{run['id']}/validate")
        assert report["passed"] and report["simulated"], report
        assert call("GET", f"/api/v1/runs/{run['id']}/report")["run_id"] == run["id"]
        results = call("GET", f"/api/v1/runs/{run['id']}/results")
        assert results["outputs"] and all(item["exists"] for item in results["outputs"]), results
        for item in results["outputs"]:
            assert call("GET", item["download_url"])
        records.append({"workflow": template["id"], "run": run, "report": report, "results": results})
    from pathlib import Path
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"passed": True, "simulated": True, "records": records}, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"PASS: {len(records)} workflows; acceptance record: {output.resolve()}")


if __name__ == "__main__":
    main()
