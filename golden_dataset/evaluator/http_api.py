from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import mimetypes
import uuid
from urllib import parse, request, error


class ApiClientError(RuntimeError):
    """Raised when API call fails."""


@dataclass
class ChatResult:
    response_text: str
    chart_jsons: list[str]
    download_url: str | None
    audit_summary: str | None


def _encode_multipart_form_data(
    fields: dict[str, str],
    files: list[tuple[str, Path]],
) -> tuple[bytes, str]:
    boundary = f"----golden-{uuid.uuid4().hex}"
    chunks: list[bytes] = []

    def append_line(text: str) -> None:
        chunks.append(text.encode("utf-8"))

    for name, value in fields.items():
        append_line(f"--{boundary}\r\n")
        append_line(f'Content-Disposition: form-data; name="{name}"\r\n\r\n')
        append_line(f"{value}\r\n")

    for field_name, file_path in files:
        file_bytes = file_path.read_bytes()
        filename = file_path.name
        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        append_line(f"--{boundary}\r\n")
        append_line(
            f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
        )
        append_line(f"Content-Type: {mime}\r\n\r\n")
        chunks.append(file_bytes)
        append_line("\r\n")

    append_line(f"--{boundary}--\r\n")
    body = b"".join(chunks)
    content_type = f"multipart/form-data; boundary={boundary}"
    return body, content_type


class AgentApiClient:
    def __init__(self, base_url: str, timeout_seconds: int = 300) -> None:
        normalized = base_url.rstrip("/")
        self.base_url = normalized
        self.timeout_seconds = timeout_seconds

    def _open(self, req: request.Request) -> bytes:
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                return resp.read()
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise ApiClientError(f"HTTP {exc.code}: {detail}") from exc
        except error.URLError as exc:
            raise ApiClientError(f"Network error: {exc}") from exc

    def upload_files(self, session_id: str, file_paths: list[Path]) -> dict[str, Any]:
        upload_url = f"{self.base_url}/upload"
        fields = {"session_id": session_id}
        files = [("files", path) for path in file_paths]
        body, content_type = _encode_multipart_form_data(fields, files)
        req = request.Request(
            upload_url,
            data=body,
            method="POST",
            headers={"Content-Type": content_type},
        )
        response_bytes = self._open(req)
        return json.loads(response_bytes.decode("utf-8"))

    def healthcheck(self) -> dict[str, Any]:
        health_url = f"{self.base_url}/health"
        req = request.Request(health_url, method="GET")
        response_bytes = self._open(req)
        data = json.loads(response_bytes.decode("utf-8"))
        if not isinstance(data, dict):
            raise ApiClientError("Invalid /health response payload.")
        return data

    def chat(self, session_id: str, prompt: str) -> ChatResult:
        chat_url = f"{self.base_url}/chat"
        payload = json.dumps({"session_id": session_id, "message": prompt}).encode("utf-8")
        req = request.Request(
            chat_url,
            data=payload,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        response_bytes = self._open(req)
        data = json.loads(response_bytes.decode("utf-8"))
        return ChatResult(
            response_text=data.get("response_text", ""),
            chart_jsons=data.get("chart_jsons", []) or [],
            download_url=data.get("download_url"),
            audit_summary=data.get("audit_summary"),
        )

    def download_file(self, relative_url: str) -> bytes:
        full_url = parse.urljoin(f"{self.base_url}/", relative_url.lstrip("/"))
        req = request.Request(full_url, method="GET")
        return self._open(req)
