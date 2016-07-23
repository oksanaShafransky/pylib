from setuptools import setup, find_packages

setup(
    name="sw_pylib",
    packages=find_packages(),
    zip_safe=True,
    requires=['invoke']
)
