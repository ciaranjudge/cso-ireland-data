from setuptools import find_packages, setup

setup(
    name='cso-data',
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    version='0.1.0',
    description='Download data from the CSO PxStat API',
    author='Department of Social Protection',
    license='EUPL',
)
