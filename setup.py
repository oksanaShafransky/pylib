from setuptools import setup, find_packages

setup(
    name='sw_pylib',
    version='1.0.0',
    packages=find_packages(),
    zip_safe=True,

    install_requires=[
        'ujson==1.33',
        'thriftpy==0.3.8',
        'boto==2.38.0',
        'snakebite==2.11.0',
        'happybase==0.9',
        'requests==2.14.2',
        'mrjob==0.5.1',
        'cryptography==1.8.2',
        'redis==2.10.5',
        'invoke==0.12.2',
        'tld',
        'consulate==0.6.0',
        'urllib3==1.16',
        'monthdelta',
        'boto3==1.5.6',
        'retry==0.9.2'
    ],
    setup_requires=[
        'pytest-runner',
        'wheel==0.29.0'
    ],
    tests_require=[
        'pytest==3.1.3',
        'ujson==1.33',
        'thriftpy==0.3.8',
        'boto==2.38.0',
        'snakebite==2.11.0',
        'happybase==0.9',
        'requests==2.14.2',
        'mrjob==0.5.1',
        'cryptography==1.8.2',
        'redis==2.10.5',
        'invoke==0.12.2',
        'python-dateutil',
        'boto3==1.5.6',
        'retry==0.9.2'
    ],
    test_suite='tests',
    entry_points={
        'console_scripts': [
            'ptask = pylib.tasks.ptask_invoke:main'
        ]
    },
    include_package_data=True
)
