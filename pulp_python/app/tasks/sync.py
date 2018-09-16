import json
import logging

from gettext import gettext as _
from urllib.parse import urljoin

from aiohttp.client_exceptions import ClientResponseError
from packaging import specifiers
from rest_framework import serializers

from pulpcore.plugin.models import Artifact, ProgressBar, Repository
from pulpcore.plugin.stages import (
    DeclarativeArtifact,
    DeclarativeContent,
    DeclarativeVersion,
    Stage
)

from pulp_python.app.models import (
    DistributionDigest,
    ProjectSpecifier,
    PinnedVersion,
    PythonPackageContent,
    PythonRemote,
)
from pulp_python.app.utils import parse_metadata

log = logging.getLogger(__name__)


def sync(remote_pk, repository_pk):
    """
    Sync content from the remote repository.

    Create a new version of the repository that is synchronized with the remote.

    Args:
        remote_pk (str): The remote PK.
        repository_pk (str): The repository PK.

    Raises:
        serializers: ValidationError

    """
    remote = PythonRemote.objects.get(pk=remote_pk)
    repository = Repository.objects.get(pk=repository_pk)

    if not remote.url:
        raise serializers.ValidationError(
            detail=_("A remote must have a url attribute to sync."))

    first_stage = PythonFirstStage(remote)
    DeclarativeVersion(first_stage, repository).create()


class PythonFirstStage(Stage):
    """
    First stage of the Asyncio Stage Pipeline.

    Create a :class:`~pulpcore.plugin.stages.DeclarativeContent` object for each content unit
    that should exist in the new :class:`~pulpcore.plugin.models.RepositoryVersion`.
    """

    def __init__(self, remote):
        """
        The first stage of a pulp_python sync pipeline.

        Args:
            remote (PythonRemote): The remote data to be used when syncing

        """
        self.remote = remote

    async def __call__(self, in_q, out_q):
        """
        Build and emit `DeclarativeContent` from the remote metadata.

        Fetch and parse the remote metadata, use the Project Specifiers on the Remote
        to determine which Python packages should be synced.

        Args:
            in_q (asyncio.Queue): Unused because the first stage doesn't read from an input queue.
            out_q (asyncio.Queue): The out_q to send `DeclarativeContent` objects to.

        """
        project_specifiers = ProjectSpecifier.objects.filter(remote=self.remote)

        with ProgressBar(message='Fetching Project Metadata') as pb:
            for project_specifier in project_specifiers:
                # Fetch the metadata from PyPI
                pb.increment()
                try:
                    metadata = await self.get_project_metadata(project_specifier.name)
                except ClientResponseError as e:
                    # Project doesn't exist, log a message and move on
                    log.info(_("HTTP 404 'Not Found' for url '{url}'\n"
                               "Does project '{name}' exist on the remote repository?").format(
                        url=e.request_info.url,
                        name=project_specifier.name
                    ))
                    continue

                # Determine which packages from the project match the criteria in the specifiers
                packages = await self.get_relevant_packages(
                    metadata=metadata,
                    includes=[
                        specifier for specifier in project_specifiers if not specifier.exclude
                    ],
                    excludes=[
                        specifier for specifier in project_specifiers if specifier.exclude
                    ],
                    pinned_version=PinnedVersion.objects.filter(remote=self.remote, name=project_specifier.name),
                    prereleases=self.remote.prereleases
                )

                # For each package, create Declarative objects to pass into the next stage
                for entry in packages:
                    url = entry.pop('url')

                    artifact = Artifact(sha256=entry.pop('sha256_digest'))
                    package = PythonPackageContent(**entry)

                    da = DeclarativeArtifact(artifact, url, entry['filename'], self.remote)
                    dc = DeclarativeContent(content=package, d_artifacts=[da])

                    await out_q.put(dc)
        await out_q.put(None)

    async def get_project_metadata(self, project_name):
        """
        Get the metadata for a given project name from PyPI.

        Args:
            project_name (str): The name of a project, e.g. "Django".

        Returns:
            dict: Python project metadata from PyPI.

        """
        metadata_url = urljoin(
            self.remote.url, 'pypi/{project}/json'.format(project=project_name)
        )
        downloader = self.remote.get_downloader(metadata_url)
        await downloader.run()
        with open(downloader.path) as metadata_file:
            return json.load(metadata_file)

    async def get_relevant_packages(self, metadata, includes, excludes, pinned_version, prereleases):
        """
        Provided project metadata and specifiers, return the matching packages.

        Compare the defined specifiers against the project metadata and create a deduplicated
        list of metadata for the packages matching the criteria.

        Args:
            metadata (dict): Metadata about the project from PyPI.
            includes (iterable): An iterable of project_specifiers for package versions to include.
            excludes (iterable): An iterable of project_specifiers for package versions to exclude.
            pinned_version (str): An iterable of project_specifiers for package versions to exclude.
            prereleases (bool): Whether or not to include pre-release package versions in the sync.

        Returns:
            list: List of dictionaries containing Python package metadata

        """
        # The set of project release metadata, in the format {"version": [package1, package2, ...]}
        releases = metadata['releases']
        # The packages we want to return
        remote_packages = []

        if pinned_version.exists():
            if releases.get(pinned_version.version, None):
                for package in releases[pinned_version.version]:
                    remote_packages.append(parse_metadata(metadata['info'], pinned_version.version, package))

        # Delete versions/packages matching the exclude specifiers.
        for exclude_specifier in excludes:
            # Shortcut: If one of the specifiers matches all versions, clear the whole releases
            # dict and quit checking (because everything has already been excluded).
            if not (exclude_specifier.version_specifier):
                releases.clear()
                break

            # We have to check all the metadata.
            for version in list(releases.keys()):  # Prevent iterator invalidation.
                specifier = specifiers.SpecifierSet(
                    exclude_specifier.version_specifier,
                    prereleases=prereleases
                )
                # First check the version specifer and delete matching packages.
                if specifier.contains(version):
                    del releases[version]

        for version, packages in releases.items():
            for include_specifier in includes:
                # Fast path: If one of the specifiers matches all versions, return all of the packages for the version.
                if prereleases:
                    for package in packages:
                        remote_packages.append(parse_metadata(metadata['info'], version, package))
                else:
                    for package in packages:
                        specifier = specifiers.SpecifierSet(
                            include_specifier.version_specifier,
                            prereleases=prereleases
                        )

                        # First check the version specifer and include matching packages.
                        if specifier.contains(version):
                            remote_packages.append(parse_metadata(metadata['info'], version, package))

        return remote_packages
