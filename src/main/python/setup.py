from setuptools import setup, find_packages

setup(
    name="sw_pylib",
    packages=find_packages(),
    zip_safe=True,
    requires=['invoke'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    test_suite='tests'
)
