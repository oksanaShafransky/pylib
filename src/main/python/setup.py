from setuptools import setup, find_packages

setup(
    name="sw_pylib",
    packages=find_packages(),
    zip_safe=True,
    requires=['invoke', 'tld', 'mrjob', 'redis'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'boto==2.42.0', 'snakebite', 'invoke==0.12.2', 'redis', 'ujson','mrjob==0.5.2'],
    test_suite='tests'
)
