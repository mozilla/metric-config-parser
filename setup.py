from setuptools import setup


def text_from_file(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


test_dependencies = [
    "coverage",
    "isort",
    "jsonschema",
    "pytest",
    "pytest-black",
    "pytest-cov",
    "pytest-flake8",
    "mypy",
    "types-futures",
    "types-pkg-resources",
    "types-protobuf",
    "types-pytz",
    "types-PyYAML",
    "types-requests",
    "types-six",
    "types-toml",
]

extras = {
    "testing": test_dependencies,
}

setup(
    name="mozilla-jetstream-config-parser",
    author="Mozilla Corporation",
    author_email="fx-data-dev@mozilla.org",
    description="Parses jetstream configuration files",
    url="https://github.com/mozilla/jetstream-config-parser",
    packages=[
        "jetstream_config_parser",
        "jetstream_config_parser.tests",
        "jetstream_config_parser.tests.integration",
    ],
    package_data={
        "jetstream_config_parser.tests": ["data/*"],
    },
    install_requires=[
        "attrs",
        "cattrs",
        "Click",
        "GitPython",
        "jinja2",
        "pytz",
        "requests",
        "toml",
    ],
    include_package_data=True,
    tests_require=test_dependencies,
    extras_require=extras,
    long_description=text_from_file("README.md"),
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
    version="2022.10.6",
)
