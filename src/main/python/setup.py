from setuptools import setup, find_packages

setup(
    name="sw_pylib",
    packages=find_packages(),
    zip_safe=True,
    install_requires=['invoke', 'tld', 'mrjob', 'redis', 'snakebite', 'invoke==0.12.2', 'redis', 'ujson==1.33','mrjob==0.5.2'],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'boto==2.42.0', 'snakebite', 'invoke==0.12.2', 'redis', 'ujson==1.33','mrjob==0.5.2'],
    test_suite='tests'
)
