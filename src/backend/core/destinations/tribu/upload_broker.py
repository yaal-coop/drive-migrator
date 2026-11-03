"""Client for upload-broker, which sends the archives to Tribu."""

import hashlib
import os

from django.conf import settings

import requests
from celery.utils.log import get_task_logger
from requests.exceptions import HTTPError

from core.destinations.drive.drive_backend import DriveMenshenTokenBackend

logger = get_task_logger(__name__)

# The complete call is slow: it builds the file and calls Tribu.
API_TIMEOUT = 60
# Each part is at least 64 MiB.
PART_TIMEOUT = (10, 600)


class UploadBrokerTokenBackend(DriveMenshenTokenBackend):
    """Get an upload-broker token from Menshen.

    Same as the Drive Menshen mode, with the upload-broker audience and scope.
    """

    @property
    def _audience(self) -> str:
        return settings.UPLOAD_BROKER_AUDIENCE

    @property
    def _scope(self) -> str:
        return settings.UPLOAD_BROKER_SCOPE

    def get_token(self) -> str:
        """Return a valid exchanged token."""
        return self._get_token()


def file_md5(path: str) -> str:
    """Return the MD5 checksum of a file. Tribu checks it after the download."""
    with open(path, "rb") as file:
        return hashlib.file_digest(
            file, lambda: hashlib.md5(usedforsecurity=False)
        ).hexdigest()


def _is_already_queued(error: HTTPError) -> bool:
    response = error.response
    if response is None or response.status_code != 409:
        return False

    try:
        body = response.json()
    except ValueError:
        return False

    return isinstance(body, dict) and body.get("detail") == "already_queued"


class UploadBrokerClient:
    """Send an archive to upload-broker for a user, part by part."""

    def __init__(self, user):
        self._tokens = UploadBrokerTokenBackend(user)
        self.session = requests.Session()

    def _post(self, path: str, payload: dict) -> requests.Response:
        response = self.session.post(
            f"{settings.UPLOAD_BROKER_URL.rstrip('/')}/uploads{path}",
            json=payload,
            headers={"Authorization": f"Bearer {self._tokens.get_token()}"},
            timeout=API_TIMEOUT,
        )
        response.raise_for_status()
        return response

    def upload(self, path: str, source_id: str, title: str, owner: dict) -> None:
        """Send the file, then ask upload-broker to send it to Tribu."""
        opened = self._post(
            "",
            {
                "source_id": source_id,
                "size": os.path.getsize(path),
                "md5": file_md5(path),
                "title": title,
                "owner": owner,
            },
        ).json()
        logger.info(
            "upload-broker: sending %s in %s parts", source_id, len(opened["parts"])
        )
        parts = self._send_parts(path, opened["part_size"], opened["parts"])
        try:
            self._post("/complete", {"ticket": opened["ticket"], "parts": parts})
        except HTTPError as error:
            if not _is_already_queued(error):
                raise

            logger.info("upload-broker: %s is already in the Tribu queue", source_id)

    def _send_parts(self, path: str, part_size: int, urls: list[dict]) -> list[dict]:
        sent = []
        with open(path, "rb") as file:
            for part in urls:
                file.seek((part["number"] - 1) * part_size)
                response = self.session.put(
                    part["url"], data=file.read(part_size), timeout=PART_TIMEOUT
                )
                response.raise_for_status()
                sent.append(
                    {"number": part["number"], "etag": response.headers["ETag"]}
                )

        return sent
