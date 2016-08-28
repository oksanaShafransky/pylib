from setuptools import setup, find_packages

setup(
    name="sw_pylib",
    packages=find_packages(),
    zip_safe=True,
    requires=['invoke', 'tld', 'mrjob', 'redis'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'snakebite', 'invoke==0.12.2', 'redis'],
    test_suite='tests'
)
