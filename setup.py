from setuptools import setup, find_packages

with open("README.md") as readme_file:
    README = readme_file.read()

setup(
    name="taransay",
    description="Taransay Database",
    long_description=README,
    version="0.1.0",
    author="Sean Leavey",
    author_email="taransaydb@attackllama.com",
    url="https://github.com/SeanDS/taransaydb/",
    packages=find_packages(),
    python_requires=">=3.7",
    extras_require={
        "dev": [
            "pytest",
            "pytest-flake8",
            "faker",
            "black",
            "pre-commit",
            "pylint",
            "flake8",
            "flake8-bugbear",
        ]
    },
    license="LGPL-3.0-or-later",
    classifiers=[
        "License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
