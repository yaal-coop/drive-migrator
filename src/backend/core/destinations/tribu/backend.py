"""Tribu destination: sends the workspace archive to Tribu through upload-broker."""

import csv
import os

from core.backends.destination import MEMBERS_CSV_FILENAME, AbstractDestinationBackend
from core.destinations.tribu.upload_broker import UploadBrokerClient
from core.models import Workspace
from core.processing.folder_helper import ArchiveManager


def get_owner(workspace, user) -> dict:
    """Return the archive owner for Tribu.

    Tribu needs the first and last names. The user model does not store them,
    so we take them from the workspace members. If the user is not in the
    members, both names are the local part of the email address.
    """
    email = user.email
    member = next(
        (
            m
            for m in workspace.members or []
            if (m.get("email") or "").lower() == email.lower()
        ),
        {},
    )
    fallback = email.split("@")[0]
    return {
        "email": email,
        "first_name": member.get("firstName") or fallback,
        "last_name": member.get("name") or fallback,
    }


class TribuDestinationBackend(AbstractDestinationBackend):
    """Zip the workspace and send the archive to Tribu.

    upload-broker stores the archive and adds it to the Tribu migration queue.
    Tribu then imports it and sends the last mail to the user.
    """

    name = "tribu"
    label = "Tribu"

    def export(self, workspace, user, local_folder_path: str) -> None:
        if not user.email:
            raise ValueError(f"User {user.id} has no email. Tribu needs it.")

        csv_path = self._write_members_csv(workspace, local_folder_path)
        manager = ArchiveManager()
        manager.zip_workspace_folder(workspace)
        if csv_path:
            os.remove(csv_path)

        try:
            UploadBrokerClient(user).upload(
                manager.get_archive_path(workspace),
                source_id=workspace.source_id,
                title=workspace.title,
                owner=get_owner(workspace, user),
            )
        finally:
            manager.delete_archive(workspace)

        workspace.set_destination_status(self.name, Workspace.Status.SUCCESS)
        workspace.save()

    @staticmethod
    def _write_members_csv(workspace, local_folder_path: str) -> str | None:
        """Write the members CSV in the local folder, like the archive destination."""
        if not workspace.members:
            return None

        csv_path = os.path.join(local_folder_path, MEMBERS_CSV_FILENAME)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(
                [m.get("name", ""), m.get("firstName", ""), m.get("email", "")]
                for m in workspace.members
            )

        return csv_path
