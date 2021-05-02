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
        'happybase==1.1.0',
        'requests==2.14.2',
        'mrjob==0.5.1',
        'cryptography==1.8.2',
        'redis==2.10.5',
        'invoke==0.12.2',
        'tld',
        'consulate==0.6.0',
        'urllib3==1.16',
        'monthdelta',
        'numpy==1.12.0'
    ],
    setup_requires=[
        # todo: understand what this does
        # 'setuptools-scm==5.0.2',
        #todo: restore pytest runner
        # 'pytest-runner==5.2',
        'wheel==0.29.0',
    ],
    tests_require=[
        'setuptools-scm==5.0.2',
        'pytest==3.1.3',
        'ujson==1.33',
        'thriftpy==0.3.8',
        'boto==2.38.0',
        'snakebite==2.11.0',
        'happybase==1.1.0',
        'requests==2.14.2',
        'mrjob==0.5.1',
        'cryptography==1.8.2',
        'redis==2.10.5',
        'invoke==0.12.2',
        'python-dateutil==2.8.0',
        'boto3==1.7.2',
        'retry==0.9.2',
        'idna==2.10'
    ],
    test_suite='tests',
    entry_points={
        'console_scripts': [
            'ptask = pylib.tasks.ptask_invoke:main'
        ]
    },
    include_package_data=True
)
