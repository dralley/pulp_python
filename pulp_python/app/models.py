from logging import getLogger

from django.db import models

from pulpcore.plugin.models import Content, Remote, Model, Publisher


log = getLogger(__name__)


PACKAGE_TYPES = (("bdist_dmg", "bdist_dmg"), ("bdist_dumb", "bdist_dumb"),
                 ("bdist_egg", "bdist_egg"), ("bdist_msi", "bdist_msi"),
                 ("bdist_rpm", "bdist_rpm"), ("bdist_wheel", "bdist_wheel"),
                 ("bdist_wininst", "bdist_wininst"), ("sdist", "sdist"))


class Classifier(Model):
    """
    Custom tags for classifier

    Fields:

        name (models.TextField): The name of the classifier

    Relations:

        python_package_content (models.ForeignKey): The PythonPackageContent this classifier
        is associated with.

    """

    name = models.TextField()
    python_package_content = models.ForeignKey("PythonPackageContent", related_name="classifiers",
                                               related_query_name="classifier",
                                               on_delete=models.CASCADE)


class PythonPackageContent(Content):
    """
    A Content Type representing Python's Distribution Package as
    defined in pep-0426 and pep-0345
    https://www.python.org/dev/peps/pep-0491/
    https://www.python.org/dev/peps/pep-0345/
    """

    TYPE = 'python'
    # Required metadata
    filename = models.TextField(unique=True, db_index=True)
    packagetype = models.TextField(choices=PACKAGE_TYPES)
    name = models.TextField()
    version = models.TextField()
    # Optional metadata
    metadata_version = models.TextField(blank=True)
    summary = models.TextField(blank=True)
    description = models.TextField(blank=True)
    keywords = models.TextField(blank=True)
    home_page = models.TextField(blank=True)
    download_url = models.TextField(blank=True)
    author = models.TextField(blank=True)
    author_email = models.TextField(blank=True)
    maintainer = models.TextField(blank=True)
    maintainer_email = models.TextField(blank=True)
    license = models.TextField(blank=True)
    requires_python = models.TextField(blank=True)
    project_url = models.TextField(blank=True)
    platform = models.TextField(blank=True)
    supported_platform = models.TextField(blank=True)
    requires_dist = models.TextField(default="[]")
    provides_dist = models.TextField(default="[]")
    obsoletes_dist = models.TextField(default="[]")
    requires_external = models.TextField(default="[]")

    class Meta:
        unique_together = (
            'filename',
        )

    def __str__(self):
        """
        Overrides Content.str to provide the distribution version and type at the end.
        e.g. <PythonPackageContent: shelf-reader [version] (whl)>
        """
        return '<{}: {} [{}] ({})>'.format(
            self._meta.object_name, self.name, self.version, self.packagetype)


class PythonPublisher(Publisher):
    """
    A Publisher for PythonContent.
    """

    TYPE = 'python'


class PythonRemote(Remote):
    """
    A Remote for Python Content.

    Attributes:
        projects (list): A list of python projects to sync
    """

    TYPE = 'python'
    projects = models.TextField(default="[]")
