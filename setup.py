from setuptools import setup, find_packages, Extension

setup(
    name="datamanifest",
    version="1.0.0",
    scripts=[
        "bin/dm",
    ],
    author="Nathan Boley",
    author_email="npboley@gmail.com",
    maintainer="Nathan Boley",
    maintainer_email="npboley@gmail.com",
    tests_require=["pytest"],
    packages=find_packages(),
    include_package_data=True,
)
