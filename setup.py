import io
import os

from pathlib import Path
from setuptools import find_packages, setup


PROJECT = "parable"


def read(*paths, **kwargs):
    content = ""
    with io.open(
        os.path.join(os.path.dirname(__file__), *paths),
        encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def get_version() -> str:
    return read(PROJECT, "VERSION")


def get_description() -> str:
    readme_path = Path(__file__).parent / "README.md"

    if not readme_path.exists():
        return """
        # Dagster

        The data orchestration platform built for productivity.
        """.strip()

    return readme_path.read_text(encoding="utf-8")


def get_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name=PROJECT,
    version=get_version(),
    author="ParableLab",
    author_email="aj@parablelab.io",
    license="Apache-2.0",
    description="Simplified data management for self-service analytics.",
    long_description=get_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/parablelab/parable",
    project_urls={
        "Homepage": "https://parablelab.io",
        "GitHub": "https://github.com/parablelab/parable",
        "Changelog": "https://github.com/parablelab/parable/releases",
        "Issue Tracker": "https://github.com/parablelab/parable/issues",
        "Twitter": "https://twitter.com/parablelab",
        "Instagram": "https://www.instagram.com/parablelab",
        "YouTube": "https://www.youtube.com/@parablelab",
        # "Slack": "https://parablelab.io/slack",
        # "Blog": "https://parablelab.io/blog",
        # "Newsletter": "https://parablelab.io/newsletter-signup",
    },
    packages=find_packages(exclude=["tests", ".github"]),
    install_requires=get_requirements("requirements.txt"),
    # extras_require={
    #     "test": get_requirements("requirements-test.txt")
    # },
    # entry_points={
    #     "console_scripts": ["project_name = project_name.__main__:main"]
    # },
)
