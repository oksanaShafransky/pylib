from setuptools import setup, find_packages

setup(
    name='sw_pylib',
    version='1.0.0',
    packages=find_packages(),
    zip_safe=True,

    install_requires=[
        'requests==2.14.2',
        'cryptography==1.8.2',
        'invoke==0.12.2',
        'tld',
        'redis',
        'python-dateutil==2.6.0',
        'happybase',
        'snakebite',
        'thriftpy==0.3.8',
        'ujson==1.33',
        'mrjob==0.5.2',
        'consulate==0.6.0',
        'urllib3==1.16',
        'monthdelta'
    ],
    setup_requires=[
        'pytest-runner',
        'wheel==0.29.0'
    ],
    tests_require=[
        'requests==2.14.2',
        'cryptography==1.8.2',
        'pytest',
        'python-dateutil==2.6.0',
        'boto==2.42.0',
        'snakebite',
        'invoke==0.12.2',
        'redis',
        'ujson==1.33',
        'mrjob==0.5.2'
    ],
    test_suite='tests',
    entry_points={
        'console_scripts': [
            'ptask = pylib.tasks.ptask_invoke:main'
        ]
    },
    include_package_data=True
)
