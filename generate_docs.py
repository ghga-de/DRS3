# Copyright 2023 Universität Tübingen, DKFZ, EMBL, and Universität zu Köln
# for the German Human Genome-Phenome Archive (GHGA)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate documentation for this package using different sources."""

import json
import subprocess  # nosec
from pathlib import Path
from string import Template

import jsonschema2md
from pydantic import BaseModel, Field
from setuptools.config.setupcfg import read_configuration
from stringcase import spinalcase, titlecase

ROOT_DIR = Path(__file__).parent.resolve()
SETUP_CFG_PATH = ROOT_DIR / "setup.cfg"
DESCRIPTION_PATH = ROOT_DIR / ".description.md"
README_TEMPLATE_PATH = ROOT_DIR / ".readme_template.md"
CONFIG_SCHEMA_PATH = ROOT_DIR / "config_schema.json"


class PackageHeader(BaseModel):
    """A basic summary of a package."""

    shortname: str = Field(
        ...,
        description=(
            "The abbreviation of the package name. Is identical to the package name."
        ),
    )
    version: str = Field(..., description="The version of the package.")
    summary: str = Field(
        ..., description="A short 1 or 2 sentence summary of the package."
    )


class PackageName(BaseModel):
    """The name of a package and it's different representations."""

    name: str = Field(..., description="The full name of the package in spinal case.")
    title: str = Field(..., description="The name of the package formatted as title.")


class PackageDetails(PackageHeader, PackageName):
    """A container for details on a package used to build documentation."""

    description: str = Field(
        ..., description="A markdown-formatted description of the package."
    )
    config_description: str = Field(
        ...,
        description=(
            "A markdown-formatted list of all configuration parameters of this package."
        ),
    )


def read_package_header() -> PackageHeader:
    """Read basic information about the package from the setup.cfg."""

    setup_config = read_configuration(SETUP_CFG_PATH)
    setup_metadata = setup_config["metadata"]
    return PackageHeader(
        shortname=setup_metadata["name"],
        version=setup_metadata["version"],
        summary=setup_metadata["description"],
    )


def read_package_name() -> PackageName:
    """Infer the package name from the name of the git origin."""

    with subprocess.Popen(
        args="basename -s .git `git config --get remote.origin.url`",
        cwd=ROOT_DIR,
        stdout=subprocess.PIPE,
        shell=True,
    ) as process:
        stdout, _ = process.communicate()

    if not stdout:
        raise RuntimeError("The name of the git origin could not be resolved.")
    git_origin_name = stdout.decode("utf-8").strip()

    return PackageName(
        name=spinalcase(git_origin_name), title=titlecase(git_origin_name)
    )


def read_package_description() -> str:
    """Read the package description."""

    return DESCRIPTION_PATH.read_text()


def generate_config_docs() -> str:
    """Generate markdown-formatted documentation for the configration parameters
    listed in the config schema."""

    parser = jsonschema2md.Parser(
        examples_as_yaml=False,
        show_examples="all",
    )
    with open(CONFIG_SCHEMA_PATH, "r", encoding="utf-8") as json_file:
        config_schema = json.load(json_file)

    md_lines = parser.parse_schema(config_schema)

    # ignore everything before the properites header:
    properties_index = md_lines.index("## Properties\n\n")
    md_lines = md_lines[properties_index + 1 :]

    return "\n".join(md_lines)


def get_package_details() -> PackageDetails:
    """Get details required to build documentation for the package."""

    header = read_package_header()
    name = read_package_name()
    description = read_package_description()
    config_description = generate_config_docs()
    return PackageDetails(
        **header.dict(),
        **name.dict(),
        description=description,
        config_description=config_description
    )


def generate_single_readme(*, details: PackageDetails) -> str:
    """Generate a single markdown-formatted readme file for the package based on the
    provided details."""

    template_content = README_TEMPLATE_PATH.read_text()
    template = Template(template_content)
    return template.substitute(details.dict())


with open("test_readme.md", "w", encoding="utf-8") as readme_file:
    readme_file.write(generate_single_readme(details=get_package_details()))
