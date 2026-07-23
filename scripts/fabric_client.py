import time
import requests

_TOKEN_URL = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
_BASE_URL = "https://api.fabric.microsoft.com/v1"
_SCOPE = "https://api.fabric.microsoft.com/.default"
_POLL_INTERVAL = 5
_POLL_TIMEOUT = 300


class FabricClient:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None

    def _get_token(self) -> str:
        if self._token:
            return self._token
        resp = requests.post(
            _TOKEN_URL.format(tenant=self._tenant_id),
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "scope": _SCOPE,
            },
            timeout=30,
        )
        resp.raise_for_status()
        self._token = resp.json()["access_token"]
        return self._token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
        }

    def _poll(self, operation_url: str) -> None:
        deadline = time.time() + _POLL_TIMEOUT
        while time.time() < deadline:
            resp = requests.get(operation_url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status")
            if status == "Succeeded":
                return
            if status == "Failed":
                error = data.get("error", {})
                raise RuntimeError(f"Operation failed: {error.get('message', data)}")
            time.sleep(_POLL_INTERVAL)
        raise TimeoutError(f"Operation did not complete within {_POLL_TIMEOUT}s")

    def get_workspace_name(self, workspace_id: str) -> str:
        resp = requests.get(
            f"{_BASE_URL}/workspaces/{workspace_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["displayName"]

    def get_workspace_items(self, workspace_id: str) -> list[dict]:
        items = []
        url = f"{_BASE_URL}/workspaces/{workspace_id}/items"
        while url:
            resp = requests.get(url, headers=self._headers(), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            items.extend(data.get("value", []))
            url = data.get("continuationUri")
        return items

    def create_item(
        self,
        workspace_id: str,
        display_name: str,
        item_type: str,
        parts: list[dict],
    ) -> None:
        resp = requests.post(
            f"{_BASE_URL}/workspaces/{workspace_id}/items",
            headers=self._headers(),
            json={
                "displayName": display_name,
                "type": item_type,
                "definition": {"parts": parts},
            },
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            return
        resp.raise_for_status()

    def get_item_definition_raw(self, workspace_id: str, item_id: str, fmt: str | None = None) -> str:
        url = f"{_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/getDefinition"
        if fmt:
            url += f"?format={fmt}"
        resp = requests.post(url, headers=self._headers(), json={}, timeout=60)
        print(f"  getDefinition status: {resp.status_code}")
        if resp.status_code == 202:
            location = resp.headers["Location"]
            print(f"  Location: {location}")
            self._poll(location)
            # Fabric LRO result lives at /result suffix, not the status URL itself
            result_resp = requests.get(location + "/result", headers=self._headers(), timeout=30)
            print(f"  /result status: {result_resp.status_code}")
            if result_resp.status_code == 200:
                return result_resp.text
            # Fallback: try the operation URL itself
            fallback = requests.get(location, headers=self._headers(), timeout=30)
            fallback.raise_for_status()
            return fallback.text
        resp.raise_for_status()
        return resp.text

    def get_item_definition(self, workspace_id: str, item_id: str, fmt: str | None = None) -> list[dict]:
        url = f"{_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/getDefinition"
        if fmt:
            url += f"?format={fmt}"
        resp = requests.post(
            url,
            headers=self._headers(),
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            resp = requests.get(resp.headers["Location"], headers=self._headers(), timeout=30)
            resp.raise_for_status()
            return resp.json().get("definition", {}).get("parts", [])
        resp.raise_for_status()
        return resp.json().get("definition", {}).get("parts", [])

    def delete_item(self, workspace_id: str, item_id: str) -> None:
        resp = requests.delete(
            f"{_BASE_URL}/workspaces/{workspace_id}/items/{item_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()

    def refresh_semantic_model(self, workspace_id: str, item_id: str) -> None:
        # DirectLake models require a refresh (framing) after deploy; without it
        # DAX queries fail with "table is not refreshed" even though the model exists.
        resp = requests.post(
            f"{_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType=DefaultJob",
            headers=self._headers(),
            json={},
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            return
        resp.raise_for_status()

    def update_item_definition(
        self,
        workspace_id: str,
        item_id: str,
        parts: list[dict],
    ) -> None:
        resp = requests.post(
            f"{_BASE_URL}/workspaces/{workspace_id}/items/{item_id}/updateDefinition",
            headers=self._headers(),
            json={"definition": {"parts": parts}},
            timeout=60,
        )
        if resp.status_code == 202:
            self._poll(resp.headers["Location"])
            return
        resp.raise_for_status()
